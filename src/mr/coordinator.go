package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// define a enum about TaskType

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
	TaskType  int
	Files     []string
	NumReduce int
}

type TaskMetaInfo struct {
	TaskPointer *Task
	Statue      TaskStatue
	StartTime   time.Time
}

type TaskContainer struct {
	taskCon map[int]*TaskMetaInfo //用指针,否则不能呢个修改其总内容
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
		taskContainer:   TaskContainer{taskCon: make(map[int]*TaskMetaInfo, len(files)+nReduce)},
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
		taskTemp.Files = append(taskTemp.Files, file)
		taskTemp.TaskId = c.getTaskId()
		taskTemp.TaskType = MapType
		taskTemp.NumReduce = c.reduceNum

		c.mapChanel <- taskTemp
		taskTempMeta := TaskMetaInfo{
			TaskPointer: taskTemp,
			Statue:      Waitting,
		}
		c.taskContainer.taskCon[taskTemp.TaskId] = &taskTempMeta

		log.Printf("job:%+v has brought  into map\n", taskTemp)
	}
	log.Println("mapchannel has been prepared!")
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()

	defer c.mu.Unlock()
	switch c.coordinateStage { //todo: 代码重构
	case MapStage:
		{
			if len(c.mapChanel) > 0 {
				*reply = *<-c.mapChanel
				c.taskContainer.taskCon[reply.TaskId].Statue = Running
				c.taskContainer.taskCon[reply.TaskId].StartTime = time.Now()
				fmt.Printf("%+v,已经从mapchanne取出，状态变为running\n", reply)
			} else {
				fmt.Println("coordinator的mapchanne里的maptask已经取完了！")
				reply.TaskType = NullType //空类型，在worker端执行task的时候，单独作处理，否则会出现，读不到了文件的情况
				if ok := c.checkCoordinator(); ok {
					c.toNextStage()
				}
			}
		}
	case ReduceStage:
		{
			if len(c.reduceChanel) > 0 {
				*reply = *<-c.reduceChanel
				c.taskContainer.taskCon[reply.TaskId].Statue = Running
				c.taskContainer.taskCon[reply.TaskId].StartTime = time.Now()
				fmt.Printf("%+v,已经从reducechanne取出，状态变为running\n", reply)
			} else {
				fmt.Println("coordinator的reducechanne里的reducetask已经取完了！")
				reply.TaskType = NullType //空类型，在worker端执行task的时候，单独作处理，否则会出现，读不到了文件的情况
				if ok := c.checkCoordinator(); ok {
					c.toNextStage()
				}
			}
		}
	case CoordinateDone:
		{
			reply.TaskType = AllDoneYype //给worker一个信息：所有任务都已经完成，可以结束进程了
		}
	}
	return nil
}

// coordinator进入下一个阶段（mapstage到reducestage或者reducestage到done）
func (c *Coordinator) toNextStage() {

	if c.coordinateStage == CoordinateDone {
		fmt.Println("coordinator 已经在done阶段了")
		return
	}

	if c.coordinateStage == MapStage { //todo：存在data race 但是不知道原因
		c.coordinateStage = ReduceStage
		fmt.Println("coordinator 从map阶段进入reduce阶段")
		//启动reduce任务
		c.initReduceTask()
	} else {
		c.coordinateStage = CoordinateDone
		fmt.Println("coordinate 从reduce阶段进入done阶段")
	}
}

// 定义一个判断coordinator状态是否为done或者mapstage结束的函数
func (c *Coordinator) checkCoordinator() bool {

	//1 统计taskcontainer里面的任务，未完成/已完成的maptask/reducetask的数目
	var doneMap, unDoneMap, doneReduce, unDoneReduce int
	for _, tempTask := range c.taskContainer.taskCon {
		if tempTask.TaskPointer.TaskType == MapType {
			if tempTask.Statue == Done {
				doneMap++
			} else {
				unDoneMap++
			}
		} else if tempTask.TaskPointer.TaskType == ReduceType {
			if tempTask.Statue == Done {
				doneReduce++
			} else {
				unDoneReduce++
			}
		}
	}
	fmt.Printf("doneMap:%d; undonemap:%d; donereduce:%d;undonereduce:%d\n\n", doneMap, unDoneMap, doneReduce, unDoneReduce)
	if doneMap > 0 && unDoneMap == 0 && doneReduce == 0 && unDoneReduce == 0 ||
		doneMap == 0 && unDoneMap == 0 && doneReduce > 0 && unDoneReduce == 0 {
		return true
	}

	return false
}

// reply从worker那边传过来参数无法到达coordinator
func (c *Coordinator) MarkTaskDone(args *Task, reply *Task) error {

	//reply.Statue = Done
	c.taskContainer.taskCon[args.TaskId].Statue = Done

	return nil
}

//1 initReduceTask()在mapstage转为reducestage时候调用

// reduce个数从coordinate里面取
func (c *Coordinator) initReduceTask() {
	//1 从当前目录下读文件,并把文件mr-out-x-y中y相等的值存在taskId=y的task的文件名数组里面

	for i := 1; i <= c.reduceNum; i++ {
		reducefiles := getreducefile(i)
		taskTemp := Task{
			TaskId:    i,
			TaskType:  ReduceType,
			NumReduce: c.reduceNum,
			Files:     reducefiles,
		}
		c.reduceChanel <- &taskTemp
		taskTempMeta := TaskMetaInfo{
			TaskPointer: &taskTemp,
			Statue:      Waitting,
		}
		c.taskContainer.taskCon[i] = &taskTempMeta
		fmt.Printf("任务:%+v已经进入reduceChannle", taskTemp)
	}
	fmt.Println("reducechannel 已经准备好了!")

}

// 取当前路径下的相关文件
func getreducefile(reducePos int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp-") && strings.HasSuffix(fi.Name(), strconv.Itoa(reducePos)) {
			s = append(s, fi.Name())
		}
	}

	return s
}
