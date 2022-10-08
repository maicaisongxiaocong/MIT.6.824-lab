package mr

import (
	"fmt"
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

type TaskStatue int

const (
	Waitting = iota
	running
	done
)

type CoordinateStage int

const (
	MapStage = iota
	ReduceStage
	CoordinateDone
)

// define a task struct
// Arguments must be capitalized for using rpc
type Task struct {
	TaskId    int
	TaskType  TaskType
	File      string
	NumReduce int
	Statue    TaskStatue
}

type TaskContainer struct {
	taskMap map[int]*Task
}

type Coordinator struct {
	// Your definitions here
	mapChanel       chan *Task
	reduceChanel    chan *Task
	reduceNum       int
	mapNum          int
	taskId          int
	taskContainer   TaskContainer
	mu              sync.Mutex
	coordinateStage CoordinateStage
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
	if c.coordinateStage == done {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		reduceChanel:    make(chan *Task, nReduce),
		mapChanel:       make(chan *Task, len(files)),
		taskId:          0,
		mapNum:          len(files),
		reduceNum:       nReduce,
		coordinateStage: MapStage,
		taskContainer:   TaskContainer{taskMap: make(map[int]*Task, len(files)+nReduce)},
	}
	fmt.Println("coordinate is prepared!")

	// Your code here.
	c.initMapChanel(files)
	c.server()
	return &c
}

func (c *Coordinator) initMapChanel(files []string) {

	for _, file := range files {
		var taskTemp = new(Task)
		taskTemp.File = file
		taskTemp.TaskId = c.getTaskId()
		taskTemp.TaskType = MapType
		taskTemp.Statue = Waitting
		taskTemp.NumReduce = c.reduceNum

		c.mapChanel <- taskTemp
		c.taskContainer.taskMap[taskTemp.TaskId] = taskTemp

		log.Printf("job:%+v has brought  into map\n", taskTemp)
	}
	log.Println("mapchannel has been prepared!")
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()

	defer c.mu.Unlock()
	switch c.coordinateStage {
	case MapStage:
		{
			if len(c.mapChanel) > 0 {
				*reply = *<-c.mapChanel
				reply.Statue = running
				fmt.Printf("%+v,已经从mapchanne取出，状态变为running\n", reply)
			} else {
				fmt.Println("coordinator的mapchanne里的maptask以及取完了！")
				if ok := c.checkCoordinator(); ok {
					c.toNextStage()
					fmt.Println("coordinator 由mapstage改为done了！ ") //todo:改为reducestage
				}
			}
			break
		}
	default: //todo:reducestage
		{
			fmt.Println("reduce has not prepared!")
		}
	}
	return nil
}

// coordinator进入下一个阶段（mapstage到reducestage或者reducestage到done）
func (c *Coordinator) toNextStage() {
	if c.coordinateStage == MapStage {
		c.coordinateStage = done
	} //todo:增加reduceStage
}

// 定义一个判断coordinator状态是否为done或者mapstage结束的函数
func (c *Coordinator) checkCoordinator() bool {
	//1 统计taskcontainer里面的任务，未完成/已完成的maptask/reducetask的数目
	var doneMap, unDoneMap, doneReduce, unDoneReduce int

	for _, tempTask := range c.taskContainer.taskMap {
		if tempTask.TaskType == MapType {
			if tempTask.Statue == done {
				doneMap++
			} else {
				unDoneMap++
			}
		} else {
			if tempTask.Statue == done {
				doneReduce++
			} else {
				unDoneReduce++
			}
		}
	}
	if doneMap > 0 && unDoneMap == 0 { //todo:判断reducestage结束

		return true
	} else {
		return false
	}
}

func (c *Coordinator) MarkTaskDone(args *ExampleArgs, reply *Task) error {
	reply.Statue = done //todo:改为reducestage
	return nil
}
