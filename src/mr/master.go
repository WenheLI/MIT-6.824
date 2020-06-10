package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus = int

const (
	IDLE TaskStatus = iota
	PROGRESS
	COMPLETE
)

type Master struct {
	// Your definitions here.
	mapTasks      map[string]TaskStatus
	reduceTasks   map[string]TaskStatus
	mapID         int
	reduceID      int
	mapTaskNum    int
	reduceTaskNum int
	requestMux    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func __max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (m *Master) assignTask(file string, _type TaskType, reply *TaskResponse) {

	reply.filename = file
	reply.taskType = _type
	if _type == MAP {
		m.mapTasks[file] = PROGRESS
		reply.id = m.mapID
		m.mapID++
	} else {
		m.reduceTasks[file] = PROGRESS
		reply.id = m.reduceID
		m.reduceID++
	}
}

// TODO
func (m *Master) heartbeat(file string, _type TaskType) {

}

func (m *Master) request(TaskArgs, reply *TaskResponse) error {
	m.requestMux.Lock()
	defer m.requestMux.Unlock()
	if m.mapTaskNum > 0 {
		for file, task := range m.mapTasks {
			if task == IDLE {
				m.assignTask(file, MAP, reply)
				m.heartbeat(file, MAP)
				return nil
			}
		}
		return errors.New("all mapped")
	}
	if m.reduceTaskNum > 0 {
		for file, task := range m.reduceTasks {
			if task == IDLE {
				m.assignTask(file, REDUCE, reply)
				m.heartbeat(file, REDUCE)
				return nil
			}
		}
		return errors.New("all mapped")
	}

	return errors.New("DONE")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapTasks = make(map[string]TaskStatus)
	m.reduceTasks = make(map[string]TaskStatus)

	for _, f := range files {
		m.mapTasks[f] = IDLE
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[fmt.Sprintf("reduce-worker-[0-9]*-%d", i)] = IDLE
	}
	m.reduceTaskNum = nReduce
	m.mapTaskNum = len(files)
	// Your code here.

	m.server()
	return &m
}
