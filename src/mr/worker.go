package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	args := FuckArgs{workerID}
	for {
		reply := FuckReply{}
		ok := call("Coordinator.Fuck", &args, &reply)
		if !ok {
			log.Fatalf("Worker get task fail!")
		}
		if reply.Exit == false {
			if reply.TaskType == 0 {
				// 没拿到任务 休息一会
				// time.Sleep(time.Millisecond)
			} else if reply.TaskType == 1 {
				// Map任务
				mapResult := mapf(reply.MapName, readFile(reply.MapName))
				// 根据ReduceCount拆分
				reduceContent := make([][]KeyValue, reply.ReduceCount)
				// 存进content
				for _, kv := range mapResult {
					key := ihash(kv.Key) % reply.ReduceCount
					reduceContent[key] = append(reduceContent[key], kv)
				}
				// 写Inter文件,nReduce有多少就写多少
				for i, content := range reduceContent {
					fileName := fmt.Sprintf(interFileName, reply.MapID, i)
					f, _ := os.Create(fileName)
					enc := json.NewEncoder(f)
					for _, line := range content {
						enc.Encode(&line)
					}
					f.Close()
				}
				// 回传Map任务完成
				taskDone(1, reply.MapName, 0)
			} else if reply.TaskType == 2 {
				// reduce任务,把所有key相同的values传给reduce函数,然后写入文件
				// 要先读入同一个reduceID的文件,然后排序,整理
				inter := make([]KeyValue, 0)
				for i := 0; i < reply.MapTaskCount; i++ {
					// 读取所有这个reduceID的文件
					fileName := fmt.Sprintf(interFileName, i, reply.ReduceID)
					reduceF, _ := os.Open(fileName)
					dec := json.NewDecoder(reduceF)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						inter = append(inter, kv)
					}
					reduceF.Close()
				}
				// 读入了所有的kv 排序
				sort.Sort(ByKey(inter))
				// 合并同类kv且写入文件
				fileName := fmt.Sprintf(outFileName, reply.ReduceID)
				outF, _ := os.Create(fileName)
				i := 0
				for i < len(inter) {
					j := i + 1
					for j < len(inter) && inter[j].Key == inter[i].Key {
						j++
					}
					var values []string
					for k := i; k < j; k++ {
						values = append(values, inter[k].Value)
					}
					output := reducef(inter[i].Key, values)
					fmt.Fprintf(outF, "%v %v\n", inter[i].Key, output)
					i = j
				}
				outF.Close()
				// 回传Task任务完成
				taskDone(2, "", reply.ReduceID)
			}
		} else {
			workerExit(workerID)
			os.Exit(1)
		}
	}
}

// worker退出 回传给master
func workerExit(workerID int) {
	args := WorkerExitArgs{workerID}
	ok := call("Coordinator.WorkerExit", &args, &None{})
	if !ok {
		log.Fatalf("Worker[%v] exit fail!", workerID)
	}
}

// 任务完成回传 [1]:map  [2]:reduce
func taskDone(taskType int, mapName string, reduceID int) {
	args := TaskDoneArgs{taskType, mapName, reduceID}
	ok := call("Coordinator.TaskDone", &args, &None{})
	if !ok {
		log.Fatalf("Worker matTaskDone fail!")
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
