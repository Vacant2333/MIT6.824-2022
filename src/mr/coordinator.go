package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files      map[string]*file
	MapDoneNum int
	reduceNum  int
	mu         sync.Mutex
	downWorker int
	// code end
}

type file struct {
	name    string
	Working bool
	MapDone bool
}

// Your code here -- RPC handlers for the worker to call.
/*
1.分Map任务给worker,worker完成之后call一个task ok(10s计时)
2.全部Map完成后开始reduce,每个key的所有values传给worker
3.写入文件mr-out-x 先排序好所有的intermediate
4.关闭所有worker后退出自己
*/

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	if c.MapDoneNum != len(c.files) {
		// Map任务没做完
		var fileName string
		for _, f := range c.files {
			if f.Working == false && f.MapDone == false {
				f.Working = true
				fileName = f.name
				break
			}
		}
		// 设置任务类型 Map
		reply.TaskType = 0
		reply.FileName = fileName
		reply.FileContents = readFile(fileName)
		// 检查超时
		go c.setMapTaskTimeOut(fileName)
	}
	c.mu.Unlock()
	return nil
}

// MapDone worker Map任务完成后回传这个函数
func (c *Coordinator) MapDone(args *Args, reply *Reply) error {
	// 写入inter 设置已完成
	c.mu.Lock()
	c.MapDoneNum++
	c.files[args.FileName].Working = false
	c.files[args.FileName].MapDone = true
	f, e := os.Create("mr-" + args.FileName[3:])
	enc := json.NewEncoder(f)
	for _, line := range args.Inter {
		enc.Encode(&line)
	}
	fmt.Println(f, e)
	f.Close()
	c.mu.Unlock()
	return nil
}

// 读取文件,返回内容
func readFile(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", fileName)
	}
	return string(content)
}

// Map任务超时检测 10s过了还没done就复原working2
func (c *Coordinator) setMapTaskTimeOut(fileName string) {
	timeOut := 10 * time.Second
	time.Sleep(timeOut)
	c.mu.Lock()
	fmt.Println("check map task timeout", fileName)
	if c.files[fileName].MapDone == false {
		fmt.Println("timeout map task", fileName)
		c.files[fileName].Working = false
		// 当那个worker已死亡
		//c.reduceNum--
	}
	c.mu.Unlock()
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
// mrcoordinator通过这个来检查是否任务已完成
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mu.Lock()
	if len(c.files) == 0 {
		ret = true
	}
	c.mu.Unlock()
	// code end
	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// new 一个coordinator,nReduce是worker的数量
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	cFiles := make(map[string]*file, 0)
	for _, fileName := range files {
		cFiles[fileName] = &file{fileName, false, false}
	}
	c.files = cFiles
	c.MapDoneNum = 0
	c.reduceNum = nReduce
	c.downWorker = 0
	// code end
	c.server()
	return &c
}
