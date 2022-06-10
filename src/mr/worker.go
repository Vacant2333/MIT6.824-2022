package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := Args{}
		reply := Reply{}
		// 请求任务
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			log.Fatalf("Master error in worker!")
		}
		if reply.TaskType == 1 {
			// Map任务
			inter := mapf(reply.FileName, reply.FileContents)
			// 处理完了Map任务 回传给master
			mapDone(reply.FileName, inter)
		} else if reply.TaskType == 2 {
			// Reduce任务
			reduceResult := reducef(reply.Key, reply.Values)
			// 处理完了Reduce 回传
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func reduceDone(key string, result string) {

}

func mapDone(fileName string, inter []KeyValue) {
	args := Args{}
	args.FileName = fileName
	args.Inter = inter
	reply := Reply{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if !ok {
		log.Fatalf("Worker MapDone error!")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
