package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Mutex锁
	lock sync.Mutex
	// Reduce任务数量
	reduceCount int
	// Worker进程ID
	workers []int
	// 当前状态,0正在做Map任务,1正在做Reduce任务,2等Worker全部退出
	status int
	// Map任务
	mapTasks map[string]*mapTask
}

// Map任务结构
type mapTask struct {
	id      int
	name    string
	working bool
	done    bool
}

// Your code here -- RPC handlers for the worker to call.
/*
1.分Map任务给worker,worker完成之后call一个task ok(10s计时)
2.全部Map完成后开始reduce,每个key的所有values传给worker
3.写入文件mr-out-x 先排序好所有的intermediate
4.关闭所有worker后退出自己
*/

// Fuck Worker访问此接口拿到一个任务
func (c *Coordinator) Fuck(n *None, reply *FuckReply) error {
	c.lock.Lock()
	// 检查状态
	if c.status == 0 {
		// Map任务没做完,找一个没做的任务给worker
		mapName := ""
		for _, task := range c.mapTasks {
			if task.working == false && task.done == false {
				mapName = task.name
				task.working = true
			}
		}
		if mapName == "" {
			log.Fatalf("Master get map task fail!")
		}
		// 回传给worker的数据
		reply.MapID = c.mapTasks[mapName].id
		reply.MapName = mapName
		reply.TaskType = 1
		reply.ReduceCount = c.reduceCount
	} else if c.status == 1 {

	}

	c.lock.Unlock()
	return nil
}

// RegisterWorker Worker访问此接口来注册到Master,传回一个id
func (c *Coordinator) RegisterWorker(n *None, workerID *int) error {
	c.lock.Lock()
	*workerID = len(c.workers)
	c.workers = append(c.workers, *workerID)
	log.Printf("Worker[%v] register to master now!", *workerID)
	c.lock.Unlock()
	return nil
}

// code end

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// mrcoordinator通过这个来检查是否所有任务已完成
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.lock.Lock()
	// todo:检查退出
	c.lock.Unlock()
	// code end
	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	// 初始化coordinator
	c.reduceCount = nReduce
	c.workers = make([]int, 0)
	c.status = 0
	// 初始化Map任务
	c.mapTasks = make(map[string]*mapTask)
	for _, fileName := range files {
		c.mapTasks[fileName] = &mapTask{len(c.mapTasks), fileName, false, false}
	}
	// code end
	c.server()
	return &c
}
