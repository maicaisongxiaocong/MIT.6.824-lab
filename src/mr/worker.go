package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		if task == nil { //nil表示call()函数调用失败
			fmt.Printf("call failed!\n")
			break
		}

		switch task.TaskType {
		case MapType:
			{
				doMap(mapf, task)
				callMarkTaskDone(task)
			}
		case ReduceType:
			{
				doReduce(reducef, task)
				callMarkTaskDone(task)
			}
		case NullType:
			{
				//这个状态表示 mapchannel或者reducechannel已经为空，但是call()函数也要返回一个空的reply过来，
			}
		case AllDoneYype: //表示coordinator已经处于Done的状态
			break
		}

	}
	fmt.Println("worker done !")
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
		if reply.TaskType != NullType && reply.TaskType != AllDoneYype {
			fmt.Printf("get a task : %+v\n", reply)
		}
		return reply
	} else {
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
func doMap(mapfunc func(filename string, contents string) []KeyValue, task *Task) {
	//take the datas from the file of the task
	var tempdata []KeyValue
	filename := task.Files[0]
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
		filename := "mr-out-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i+1)
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
	ok := call("Coordinator.MarkTaskDone", reply, reply) //第二个参数的值传不到coordinator，
	if ok == false {
		fmt.Println("CallMarkTaskDone fail!")
	} else {
		if reply.TaskType == MapType {
			fmt.Printf("map任务%+v已经完成！\n\n", reply)
		}

		if reply.TaskType == ReduceType {
			fmt.Printf("reduce任务%+v已经完成！\n\n", reply)

		}

	}
}

func doReduce(reducefunc func(key string, values []string) string, task *Task) {

	//将task的files读进values string[]
	var values []KeyValue
	for _, file := range task.Files {
		fi, err := os.Open(file)
		if err != nil {
			fmt.Printf("file:%v打开失败,原因:%v", file, err)
		}
		dec := json.NewDecoder(fi)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			values = append(values, kv)
		}
		fi.Close()
	}
	//对字符串进行排序
	sort.Sort(ByKey(values)) //todo:int()表示什么?

	//统计相同单词的起始和结束下标
	outFileName := "out-put-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(outFileName)
	i := 0
	for i < len(values) {
		j := i + 1
		for j < len(values) && values[j].Key == values[i].Key {
			j++
		}
		tempValues := []string{}
		for k := i; k < j; k++ {
			tempValues = append(tempValues, values[k].Value)
		}
		output := reducefunc(values[i].Key, tempValues)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", values[i].Key, output)
		i = j
	}
	fmt.Println("生成一个最终文件：", outFileName)
	ofile.Close()
}
