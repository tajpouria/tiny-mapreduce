package utils

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const MasterSockName = "/tmp/mapreduce-master"

func GetWorkerSockName(workerId int) string {
	return fmt.Sprintf("/tmp/mapreduce-worker-%d", workerId)
}

type TaskStatus int
type TaskFuncType int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

const (
	MapFunc TaskFuncType = iota
	ReduceFunc
)

type MapOutput struct {
	Addr string
	Size int64
}

type Task struct {
	ID          int
	WorkerID    int
	Status      TaskStatus
	FuncType    TaskFuncType
	InputAddr   string
	InputOffset int64
	InputLength int64
	StartedAt   time.Time

	MapOutputs []MapOutput
}

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	Task Task
}

type CompleteTaskArgs struct {
	Task Task
}

type GetMapOutputArgs struct {
	Task Task
}

type GetMapOutputReply struct {
	Res []KeyVal
}

type OkReply struct{}

func RpcServer(rcvr any, sockname string) {
	err := rpc.Register(rcvr)
	if err != nil {
		log.Fatalf("cannot register the rpc receiver %v", err)
	}
	rpc.HandleHTTP()
	os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("cannot listen on address %v %v", sockname, err)
	}
	go http.Serve(listener, nil)
	log.Printf("HTTP server is listening on unix address %v", sockname)
}

func RpcCall(sockname string, rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
