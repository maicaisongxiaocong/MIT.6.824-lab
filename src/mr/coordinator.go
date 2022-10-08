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
	NullType //当mapchannel或者reducechannel里面的数据取完了，但是还有进程去，就把需要传递的空任务状态设置为Null，避免对空任务进行操作
	AllDoneYype
)

type TaskStatue int

const (
	Waitting = iota
	Running
	Done
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
	if c.coordinateStage == CoordinateDone {
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
				//todo:改为worker执行完domap()后再改状态
				reply.Statue = Running
				c.taskContainer.taskMap[reply.TaskId].Statue = Running
				fmt.Printf("%+v,已经从mapchanne取出，状态变为running\n", reply)
			} else {
				fmt.Println("coordinator的mapchanne里的maptask已经取完了！")
				reply.TaskType = NullType //空类型，在worker端执行task的时候，单独作处理，否则会出现，读不到了文件的情况
				if ok := c.checkCoordinator(); ok {
					c.toNextStage()
					fmt.Println("coordinator 由mapstage改为done了！ ") //todo:改为reducestage
				}
			}
		}
	case CoordinateDone: //todo: reducestage
		{
			reply.TaskType = AllDoneYype //给worker一个信息：所有任务都已经完成，可以结束进程了
		}
	}
	return nil
}

// coordinator进入下一个阶段（mapstage到reducestage或者reducestage到done）
func (c *Coordinator) toNextStage() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.coordinateStage == MapStage {
		c.coordinateStage = CoordinateDone
		fmt.Println("coordinator 进入done阶段")
	} //todo:增加reduceStage
}

// 定义一个判断coordinator状态是否为done或者mapstage结束的函数
func (c *Coordinator) checkCoordinator() bool { //todo: reducestage

	//1 统计taskcontainer里面的任务，未完成/已完成的maptask/reducetask的数目
	var doneMap, unDoneMap, doneReduce, unDoneReduce int
	for _, tempTask := range c.taskContainer.taskMap {
		if tempTask.TaskType == MapType {
			if tempTask.Statue == Done {
				doneMap++
			} else {
				unDoneMap++
			}
		} else {
			if tempTask.Statue == Done {
				doneReduce++
			} else {
				unDoneReduce++
			}
		}
	}
	// fmt.Printf("doneMap:%d; undonemap:%d\n", doneMap, unDoneMap)
	if doneMap > 0 && unDoneMap == 0 { //todo:判断reducestage结束
		return true
	} else {
		return false
	}
}

// reply从worker那边传过来参数无法到达coordinator
func (c *Coordinator) MarkTaskDone(args *Task, reply *Task) error {
	//reply.Statue = Done //todo:改为reducestage
	c.taskContainer.taskMap[args.TaskId].Statue = Done
	return nil
}
