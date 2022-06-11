package mr

import (
	"fmt"
	"io/ioutil"
	"os"
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
	workerID := register()
	for {
		reply := FuckReply{}
		ok := call("Coordinator.Fuck", &None{}, &reply)
		if !ok {
			log.Fatalf("Worker get task fail!")
		}
		if reply.Exit == false {
			if reply.TaskType == 1 {
				// Map任务
				mapResult := mapf(reply.MapName, readFile(reply.MapName))
				// 根据ReduceCount拆分
				reduceContent := make([][]KeyValue, reply.ReduceCount)

				for _, kv := range mapResult {
					key := ihash(kv.Key) % reply.ReduceCount
					reduceContent[key] = append(reduceContent[key], kv)
				}
			}
		} else {
			// todo:回传exit信号
			log.Fatalf("Worker[%v] exit!", workerID)
		}
	}
}

// 注册当前worker到master,返回master给的id
func register() int {
	var workerID int
	ok := call("Coordinator.RegisterWorker", &None{}, &workerID)
	if !ok {
		log.Fatalf("Worker register to master fail!")
	}
	return workerID
}

// 读取文件,返回内容
func readFile(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Master cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Master cannot read %v", fileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("Master cannot close %v", fileName)
	}
	return string(content)
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
