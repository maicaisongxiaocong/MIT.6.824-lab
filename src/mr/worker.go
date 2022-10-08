package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//1 取任务，并判断是maptask还是reducetask
	//2.1 若是maptask则调用doMap(),并且将任务状态改为Done
	flag := true
	for flag {
		task := GetTask()
		if task == nil { //mapchannel里面没有任务了
			break
		}

		if task.TaskType == MapType {
			DoMap(mapf, task)
			callMarkTaskDone(task)
		} else {
			//2.2 todo:reduceType
			println("reduce has not defined!")
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func GetTask() *Task {
	args := ExampleArgs{}
	reply := new(Task)
	ok := call("Coordinator.DistributeTask", &args, reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("get a maptask : %+v\n", reply)
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

/*
Map任务的处理流程：
1,通过任务的fileName读取文件内容
2,对读取的数据进行map处理生成中间key-value对
3,对相同的key数据进行压缩(省略)
4,创建一个二维数组，将map处理后生成的中间key-value对通过分区函数存储到不同的分区数组中
5,为每个分区创建一个文件，并将当前分区的数据全部存入文件
*/
func DoMap(mapfunc func(filename string, contents string) []KeyValue, task *Task) {
	//take the datas from the file of the task
	var tempdata []KeyValue
	filename := task.File
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("can not open %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	file.Close()

	//call mapfunc
	res := mapfunc(filename, string(content))

	tempdata = append(tempdata, res...)

	//create different arrays for different reduces to deal with respectively
	interdata := make([][]KeyValue, task.NumReduce)
	for _, kv := range tempdata {
		key := ihash(kv.Key) % task.NumReduce
		interdata[key] = append(interdata[key], kv)
	}

	//create different output file for different reduce
	for i := 0; i < task.NumReduce; i++ {
		//outfilename = outfilename + string(task.TaskId) + string(i) //string(fid)并不能！(因为该转换会将数字直接转换为该数字对应的内码)
		filename := "mr-out-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(filename)
		enc := json.NewEncoder(ofile)
		for _, kv := range interdata[i] {
			enc.Encode(kv) //has contained '/n'
		}
		fmt.Println("生成一个一个中间文件：", filename)
		ofile.Close()
	}

}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func LoadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	//fmt.Println(err)
	if err != nil {
		log.Fatalf("cannot load plugin %v,err:", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func callMarkTaskDone(reply *Task) {
	args := ExampleArgs{}
	if err := call("Coordinator.MarkTaskDone", &args, reply); err != true {
		fmt.Println("CallMarkTaskDone fail!")
	} else {
		fmt.Printf("任务%+v已经完成！\n", Task{}) //todo:改为已经进入reducestage
	}
}
