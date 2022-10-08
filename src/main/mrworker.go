package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"os"
)
import "fmt"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	//mapf, reducef := loadPlugin(os.Args[1])
	mapf, reducef := mr.LoadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
	//task := mr.GetTask()
	//mr.DoMap(mapf, task)
	fmt.Println("worker done!")
}
