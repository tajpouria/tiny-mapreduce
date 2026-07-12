package main

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"
	"tiny-mapreduce/utils"
)

const (
	pollInterval  = 20 * time.Millisecond
	maxDialMisses = 100
)

type Worker struct {
	mu             sync.Mutex
	id             int
	mapFunc        func(string, string) []utils.KeyVal
	reduceFunc     func(string, []string) string
	nReduce        int
	partitionFiles [][]string
}

func NewWorker(pluginPath string) *Worker {
	mapFunc, reduceFunc := utils.LoadFuncsPlugin(pluginPath)
	nReduce := utils.NumReduceTask()
	return &Worker{
		id:             os.Getegid(),
		mapFunc:        mapFunc,
		reduceFunc:     reduceFunc,
		nReduce:        nReduce,
		partitionFiles: make([][]string, nReduce),
	}
}

func (w *Worker) Run() {
	misses := 0
	connected := false
	for {
		var reply utils.GetTaskReply
		args := utils.GetTaskArgs{WorkerID: w.id}
		if err := utils.RpcCall(utils.MasterSockName, "Master.GetTask", args, &reply); err != nil {
			if !connected {
				time.Sleep(pollInterval)
				continue
			}
			if misses++; misses > maxDialMisses {
				log.Printf("info master unreachable, worker %d exiting: %v", w.id, err)
				return
			}
			time.Sleep(pollInterval)
			continue
		}
		connected = true
		misses = 0

		switch t := reply.Task; {
		case t.ID == -1:
			time.Sleep(pollInterval)
		case t.FuncType == utils.MapFunc:
			w.runMap(t)
		case t.FuncType == utils.ReduceFunc:
			w.runReduce(t)
		}
	}
}

func (w *Worker) GetMapOutput(args *utils.GetMapOutputArgs, reply *utils.GetMapOutputReply) error {
	w.mu.Lock()
	var files []string
	if p := args.Task.ID; p >= 0 && p < len(w.partitionFiles) {
		files = append(files, w.partitionFiles[p]...)
	}
	w.mu.Unlock()

	var sources []kvSource
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		sources = append(sources, decoderSource(json.NewDecoder(f)))
	}

	kwayMerge(sources, func(kv utils.KeyVal) { reply.Res = append(reply.Res, kv) })
	return nil
}

func (w *Worker) runMap(t utils.Task) {
	log.Printf("info: worker %d running map task %s (%s)", w.id, t.ID, t.InputAddr)
	kvs := w.mapFunc(t.InputAddr, readSplit(t))

	buckets := make([][]utils.KeyVal, w.nReduce)
	for _, kv := range kvs {
		p := partition(kv.Key, w.nReduce)
		buckets[p] = append(buckets[p], kv)
	}
	w.flushPartitions(t.ID, buckets)

	w.complete(t)
}

func (w *Worker) runReduce(t utils.Task) {
	log.Printf("info: worker %d running reduce task %d", w.id, t.ID)

	var sources []kvSource
	for _, mo := range t.MapOutputs {
		var reply utils.GetMapOutputReply
		args := utils.GetMapOutputArgs{Task: t}
		if err := utils.RpcCall(mo.Addr, "Worker.GetMapOutput", args, &reply); err != nil {
			log.Fatalf("cannot fetch partition %d from %v: %v", t.ID, mo.Addr, err)
		}
		sources = append(sources, sliceSource(reply.Res))
	}

	tmp := fmt.Sprintf("mapreduce-out-%d.tmp", t.ID)
	f, err := os.Create(tmp)
	if err != nil {
		log.Fatalf("cannot crate output file: %v", err)
	}
	bw := bufio.NewWriter(f)

	var curKey string
	var curVals []string

	haveKey := false
	emitGroup := func() {
		if haveKey {
			fmt.Fprintf(
				bw,
				"%v %v\n",
				curKey,
				w.reduceFunc(curKey, curVals),
			)
		}
	}
	kwayMerge(sources, func(kv utils.KeyVal) {
		if haveKey && kv.Key == curKey {
			curVals = append(curVals, kv.Val)
			return
		}
		emitGroup()
		curKey, curVals, haveKey = kv.Key, append(curVals[:0], kv.Val), true
	})
	emitGroup()

	if err := bw.Flush(); err != nil {
		log.Fatalf("cannot write output file: %v", err)
	}
	f.Close()

	if err := os.Rename(tmp, fmt.Sprintf("mapreduce-out-%d", t.ID)); err != nil {
		log.Fatalf("cannot publish output file: %v", err)
	}

	w.complete(t)
}

func (w *Worker) flushPartitions(mapTaskID int, buckets [][]utils.KeyVal) {
	for p, kvs := range buckets {
		if len(kvs) == 0 {
			continue
		}

		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

		name := fmt.Sprintf("mapreduce-int-%d-%d-%d", w.id, mapTaskID, p)
		f, err := os.Create(name)
		if err != nil {
			log.Fatalf("cannot create intermediate file: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			if err := enc.Encode(kv); err != nil {
				log.Fatalf("cannot write intermediate file %v", err)
			}
		}
		if err := f.Close(); err != nil {
			log.Fatalf("cannot close intermediate file: %v", err)
		}

		w.mu.Lock()
		w.partitionFiles[p] = append(w.partitionFiles[p], name)
		w.mu.Unlock()
	}
}

func (w *Worker) complete(t utils.Task) {
	args := utils.CompleteTaskArgs{Task: t}
	if err := utils.RpcCall(utils.MasterSockName, "Master.CompleteTask", args, &utils.OkReply{}); err != nil {
		log.Printf("info: could not report completion of task %d: %v", t.ID, err)
	}
}

func readSplit(t utils.Task) string {
	f, err := os.Open(t.InputAddr)
	if err != nil {
		log.Fatalf("cannot open the input file %v: %v", t.InputAddr, err)
	}
	defer f.Close()

	buf := make([]byte, t.InputLength)
	if _, err := f.ReadAt(buf, t.InputOffset); err != nil && err != io.EOF {
		log.Fatalf("cannot read split from %v: %v", t.InputAddr, err)
	}
	return string(buf)
}

func partition(key string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % n
}

type kvSource func() (utils.KeyVal, bool)

func sliceSource(kvs []utils.KeyVal) kvSource {
	i := 0
	return func() (utils.KeyVal, bool) {
		if i >= len(kvs) {
			return utils.KeyVal{}, false
		}
		kv := kvs[i]
		i++
		return kv, true
	}
}

func kwayMerge(sources []kvSource, emit func(utils.KeyVal)) {
	h := &mergeHeap{}
	for _, next := range sources {
		if kv, ok := next(); ok {
			heap.Push(h, mergeItem{kv: kv, next: next})
		}
	}
	for h.Len() > 0 {
		it := heap.Pop(h).(mergeItem)
		emit(it.kv)
		if kv, ok := it.next(); ok {
			heap.Push(h, mergeItem{kv: kv, next: it.next})
		}
	}
}

type mergeItem struct {
	kv   utils.KeyVal
	next kvSource
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].kv.Key < h[j].kv.Key }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

func decoderSource(dec *json.Decoder) kvSource {
	return func() (utils.KeyVal, bool) {
		var kv utils.KeyVal
		if err := dec.Decode(&kv); err == io.EOF {
			return utils.KeyVal{}, false
		} else if err != nil {
			log.Fatal("cannot read intermediate file %v", err)
		}
		return kv, true
	}
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintf(os.Stderr, "usage: mapreduce-worker <funcsplugin>.so")
		os.Exit(1)
	}

	w := NewWorker(os.Args[1])
	utils.RpcServer(w, utils.GetWorkerSockName(w.id))
	w.Run()
}
