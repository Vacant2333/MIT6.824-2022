package mr

import (
	"log"
	"sync"
	"time"
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
	// Map已完成数量
	mapTaskDoneCount int

	// Reduce任务
	reduceTasks []*reduceTask
	// Reduce任务完成数量
	reduceTaskDoneCount int
}

// Reduce任务结构
type reduceTask struct {
	id       int
	working  bool
	done     bool
	workerID int
}

// Map任务结构
type mapTask struct {
	id       int
	name     string
	working  bool
	done     bool
	workerID int
}

// Your code here -- RPC handlers for the worker to call.
/*
1.分Map任务给worker,worker完成之后call一个task ok(10s计时)
2.全部Map完成后开始reduce,每个key的所有values传给worker
3.写入文件mr-out-x 先排序好所有的intermediate
4.关闭所有worker后退出自己
*/

// Fuck Worker访问此接口拿到一个任务
func (c *Coordinator) Fuck(args *FuckArgs, reply *FuckReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// 检查状态
	if c.status == 0 {
		// Map任务没做完,找一个没做的任务给worker
		mapName := ""
		for _, task := range c.mapTasks {
			if task.working == false && task.done == false {
				// 找到了可以给他的map任务
				mapName = task.name
				task.workerID = args.WorkerID
				task.working = true
				break
			}
		}
		if mapName == "" {
			// 可能找不到可以做的map任务,就传0让worker等一会
			reply.TaskType = 0
			return nil
		}
		// 回传给worker的数据
		reply.MapID = c.mapTasks[mapName].id
		reply.MapName = mapName
		reply.TaskType = 1
		reply.ReduceCount = c.reduceCount
		go c.checkTaskTimeOut(1, mapName, 0)
	} else if c.status == 1 {
		// Map做完了,在做Reduce任务
		reduceID := -1
		for i, task := range c.reduceTasks {
			if task.working == false && task.done == false {
				// 找到了可以给他的reduce任务
				reduceID = i
				task.workerID = args.WorkerID
				task.working = true
				break
			}
		}
		if reduceID == -1 {
			// 没找到可以做的reduce任务,也传0
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 2
		reply.ReduceID = reduceID
		reply.MapTaskCount = c.mapTaskDoneCount
		go c.checkTaskTimeOut(2, "", reduceID)
	} else if c.status == 2 {
		// 发送退出信号,任务都完成了
		reply.Exit = true
	}
	return nil
}

// WorkerExit Worker退出回传
func (c *Coordinator) WorkerExit(args *WorkerExitArgs, n *None) error {
	c.lock.Lock()
	log.Printf("Worker[%v] exit!", args.WorkerID)
	c.deleteWorker(args.WorkerID)
	c.lock.Unlock()
	return nil
}

// 删除worker,需要提前lock
func (c *Coordinator) deleteWorker(workerID int) {
	// 从workers删除这个worker
	workerKey := -1
	for i, worker := range c.workers {
		if worker == workerID {
			workerKey = i
			break
		}
	}
	// 检查是否有这个worker,可能这是以前没死完的worker
	if workerKey == -1 {
		log.Printf("Worker[%v] exit error! its not my worker!", workerID)
	} else {
		// 删除这个worker
		c.workers = append(c.workers[:workerKey], c.workers[workerKey+1:]...)
	}
}

// checkTaskTimeOut 检查任务超时
func (c *Coordinator) checkTaskTimeOut(taskType int, mapName string, reduceID int) {
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	if taskType == 1 {
		// 检查map任务
		if c.mapTasks[mapName].done == false {
			log.Printf("Map task[%v] dead, worker[%v] dead!:", mapName, c.mapTasks[mapName].workerID)
			c.mapTasks[mapName].working = false
			c.deleteWorker(c.mapTasks[mapName].workerID)
		}
	} else if taskType == 2 {
		// 检查reduce任务
		if c.reduceTasks[reduceID].done == false {
			log.Printf("Reduce task[%v] dead, worker[%v] dead!", reduceID, c.reduceTasks[reduceID].workerID)
			c.reduceTasks[reduceID].working = false
			c.deleteWorker(c.reduceTasks[reduceID].workerID)
		}
	}
	c.lock.Unlock()
}

// TaskDone worker任务完成回传
func (c *Coordinator) TaskDone(args *TaskDoneArgs, n *None) error {
	c.lock.Lock()
	if args.TaskType == 1 {
		// Map任务完成
		c.mapTasks[args.MapName].done = true
		c.mapTasks[args.MapName].working = false
		c.mapTaskDoneCount++
		log.Printf("Map task[%v] done", args.MapName)
		if c.mapTaskDoneCount == len(c.mapTasks) {
			// 所有Map任务已完成
			c.status = 1
			log.Println("All map tasks done!")
		}
	} else if args.TaskType == 2 {
		// Reduce任务完成
		c.reduceTasks[args.ReduceID].done = true
		c.reduceTasks[args.ReduceID].working = false
		c.reduceTaskDoneCount++
		log.Printf("Reduce Task[%v] done", args.ReduceID)
		if c.reduceTaskDoneCount == len(c.reduceTasks) {
			// 所有Reduce任务已完成
			c.status = 2
			log.Printf("All reduce tasks done!")
		}
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
	if c.status == 2 && len(c.workers) == 0 {
		log.Printf("Master done now!")
		ret = true
	}
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
	for i, fileName := range files {
		c.mapTasks[fileName] = &mapTask{i, fileName, false, false, 0}
	}
	// 初始化Reduce任务
	c.reduceTasks = make([]*reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &reduceTask{i, false, false, 0}
	}
	// code end
	c.server()
	return &c
}
