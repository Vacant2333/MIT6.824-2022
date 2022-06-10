package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files       map[string]*file
	MapDoneNum  int
	reduceNum   int
	mu          sync.Mutex
	downWorker  int
	reduceTasks []reduceTask
	// code end
}

type reduceTask struct {
	key        string
	values     []string
	working    bool
	reduceDone bool
}

type file struct {
	name       string
	Working    bool
	MapDone    bool
	ReduceName string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		reply.TaskType = 1
		reply.FileName = fileName
		reply.FileContents = readFile(fileName)
		fmt.Println("Send Map Task", fileName)
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
	fName := "mr-" + args.FileName[3:]
	f, e := os.Create(fName)
	enc := json.NewEncoder(f)
	for _, line := range args.Inter {
		enc.Encode(&line)
	}
	fmt.Println("Map task done", f.Name(), e)
	c.files[args.FileName].ReduceName = fName
	f.Close()

	if c.MapDoneNum == len(c.files) {
		// Map任务做完了 集合所有的数据用来reduce
		c.afterMapDone()
	}

	c.mu.Unlock()
	return nil
}

// 集合所有的数据用来reduce
func (c *Coordinator) afterMapDone() {
	inter := make([]KeyValue, 0)
	for _, file := range c.files {
		f, _ := os.Open(file.ReduceName)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(kv); err != nil {
				break
			}
			inter = append(inter, kv)
		}
	}
	sort.Sort(ByKey(inter))

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
		c.reduceTasks = append(c.reduceTasks, reduceTask{inter[i].Key, values, false, false})
		//output := reducef(inter[i].Key, values)
		i = j
	}
	fmt.Println(c.reduceTasks)
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
		fmt.Println("timeout map task !!!!!", fileName)
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
	// 检查退出主进程

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
	cFiles := make(map[string]*file)
	for _, fileName := range files {
		cFiles[fileName] = &file{fileName, false, false, ""}
	}
	c.files = cFiles
	c.MapDoneNum = 0
	c.reduceNum = nReduce
	c.downWorker = 0
	c.reduceTasks = make([]reduceTask, 0)
	// code end
	c.server()
	return &c
}
