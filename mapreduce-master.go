package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"tiny-mapreduce/utils"
)

type Master struct {
	mu          sync.Mutex
	mapTasks    []*utils.Task
	reduceTasks []*utils.Task

	taskTimeout   time.Duration
	checkInterval time.Duration
}

func NewMaster(inputFiles []string) *Master {
	m := &Master{
		taskTimeout:   10 * time.Second,
		checkInterval: 1 * time.Second,
	}

	for _, t := range buildMapTasks(inputFiles, utils.NumMapTask()) {
		m.mapTasks = append(m.mapTasks, &t)
	}

	for i := 0; i < utils.NumReduceTask(); i++ {
		m.reduceTasks = append(m.reduceTasks,
			&utils.Task{
				ID:       i,
				Status:   utils.Idle,
				FuncType: utils.ReduceFunc,
			},
		)
	}

	m.startRequeueLoop()
	return m
}

func (m *Master) startRequeueLoop() {
	go func() {
		for {
			time.Sleep(m.checkInterval)
			m.requeueStalled()
		}
	}()
}

func (m *Master) requeueStalled() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, group := range [][]*utils.Task{m.mapTasks, m.reduceTasks} {
		for _, t := range group {
			if t.Status == utils.InProgress && now.Sub(t.StartedAt) > m.taskTimeout {
				log.Printf(
					"info: requeuing stale task %d from worker $d after %v",
					t.ID,
					t.WorkerID,
					now.Sub(t.StartedAt).Round(time.Second),
				)
				t.Status = utils.Idle
				t.WorkerID = 0
			}
		}
	}
}

func buildMapTasks(inputFiles []string, m int) []utils.Task {
	sizes := make([]int64, len(inputFiles))
	var total int64
	for i, f := range inputFiles {
		info, err := os.Stat(f)
		if err != nil {
			log.Fatalf("cannot get stat of the input file %v: %v", f, err)
		}
		sizes[i] = info.Size()
		total += info.Size()
	}

	target := max(total/int64(m), 1)

	var tasks []utils.Task
	for i, f := range inputFiles {
		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("cannot open the input file %v: %v", f, err)
		}

		var start int64
		for start < sizes[i] {
			end := snapToNewLine(file, min(start+target), sizes[i])
			tasks = append(tasks, utils.Task{
				ID:          len(tasks),
				Status:      utils.Idle,
				FuncType:    utils.MapFunc,
				InputAddr:   f,
				InputOffset: start,
				InputLength: end - start,
			})
			start = end
		}
		file.Close()
	}

	return tasks
}

func snapToNewLine(f *os.File, pos, size int64) int64 {
	if pos > size {
		return size
	}

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		log.Fatalf("cannot seek in input file: %v")
	}

	chunk, err := bufio.NewReader(f).ReadBytes('\n')
	if err != nil && err != io.EOF {
		log.Fatalf("cannot read split boundary: %v", err)
	}

	pos += int64(len(chunk))

	return min(pos, size)
}

func assignFrom(tasks []*utils.Task, workerID int) (utils.Task, bool) {
	for _, t := range tasks {
		if t.Status == utils.Idle {
			t.Status = utils.InProgress
			t.WorkerID = workerID
			t.StartedAt = time.Now()
			return *t, true
		}
	}
	return utils.Task{}, false
}

func allDone(tasks []*utils.Task) bool {
	for _, t := range tasks {
		if t.Status != utils.Done {
			return false
		}
	}
	return true
}

func nilTask() utils.Task {
	return utils.Task{ID: -1}
}

func (m *Master) assign(workerID int) utils.Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := assignFrom(m.mapTasks, workerID); ok {
		log.Printf("info: assigned map task %d to worker %d", t.ID, workerID)
		return t
	}

	if allDone(m.mapTasks) {
		if t, ok := assignFrom(m.reduceTasks, workerID); ok {
			t.MapOutputs = m.mapWorkerLocations()
			log.Printf("info: assigned reduce task %d to worker %d", t.ID, workerID)
			return t
		}
	}

	return nilTask()
}

func (m *Master) mapWorkerLocations() []utils.MapOutput {
	seen := make(map[int]bool)
	var locs []utils.MapOutput
	for _, t := range m.mapTasks {
		if !seen[t.WorkerID] {
			seen[t.WorkerID] = true
			locs = append(
				locs,
				utils.MapOutput{Addr: utils.GetWorkerSockName(t.WorkerID)},
			)
		}
	}
	return locs
}

func (m *Master) complete(t utils.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks := m.mapTasks
	if t.FuncType == utils.ReduceFunc {
		tasks = m.reduceTasks
	}
	if t.ID < 0 || t.ID >= len(tasks) {
		return
	}

	if stored := tasks[t.ID]; stored.Status == utils.InProgress && stored.WorkerID == t.WorkerID {
		stored.Status = utils.Done
		stored.MapOutputs = t.MapOutputs
	}
}

func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return allDone(m.mapTasks) && allDone(m.reduceTasks)
}

func (m *Master) GetTask(args *utils.GetTaskArgs, reply *utils.GetTaskReply) error {
	reply.Task = m.assign(args.WorkerID)
	return nil
}

func (m *Master) CompleteTask(args *utils.CompleteTaskArgs, reply *utils.OkReply) error {
	m.complete(args.Task)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: mapreduce-master <inputfiles>...")
		os.Exit(1)
	}

	m := NewMaster(os.Args[1:])
	utils.RpcServer(m, utils.MasterSockName)

	for m.Done() == false {
		time.Sleep(20 * time.Second)
	}

	log.Println("info: all tasks completed, master exiting")
}
