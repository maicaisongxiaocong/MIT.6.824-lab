package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// define a enum about TaskType
type TaskType int

const (
	MapType = iota
	ReduceType
)

// define a task struct
// Arguments must be capitalized for using rpc
type Task struct {
	TaskId   int
	TaskType TaskType
	File     string
}

type Coordinator struct {
	// Your definitions here
	mapChanel    chan *Task
	reduceChanel chan *Task
	reduceNum    int
	mapNum       int
	taskId       int
	mu           sync.Mutex
}

// generate a task Id
func (c *Coordinator) getTaskId() int {
	i := c.taskId
	i++
	c.taskId = i
	return i

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		reduceChanel: make(chan *Task, nReduce),
		mapChanel:    make(chan *Task, len(files)),
		taskId:       0,
		mapNum:       len(files),
		reduceNum:    nReduce,
	}

	// Your code here.
	c.initMapChanel(files)
	log.Println("coordinator is prepared")
	c.server()
	return &c
}

func (c *Coordinator) initMapChanel(files []string) {

	for _, file := range files {
		var taskTemp = new(Task)
		taskTemp.File = file
		taskTemp.TaskId = c.getTaskId()
		taskTemp.TaskType = MapType
		c.mapChanel <- taskTemp
		log.Println("map is been inited :", taskTemp)
	}
	log.Println("all maps have been inited")
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()

	defer c.mu.Unlock()
	if len(c.mapChanel) > 0 {
		reply = <-c.mapChanel
		log.Println("reply:", reply)
	}
	return nil
}
