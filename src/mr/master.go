package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus = int

const (
	IDLE TaskStatus = iota
	PROGRESS
	COMPLETE
)

type TaskType = int

const (
	MAP    TaskType = 0
	REDUCE          = 1
)

type Task struct {
	taskType   TaskType
	taskStatus TaskStatus
	id         int
	fileName   string
}

type Master struct {
	// Your definitions here.
	files   []string
	taskCh  chan Task
	done    bool
	nReduce int
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

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.files = files
	m.nReduce = nReduce

	m.taskCh = make(chan Task, __max(nReduce, len(files)))

	// Your code here.

	m.server()
	return &m
}
