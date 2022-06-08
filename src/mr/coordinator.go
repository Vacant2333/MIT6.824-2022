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
	// Your definitions here.
	files     []string
	reduceNum int
}

var mx sync.Mutex

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetFile(args *Args, reply *Reply) error {
	mx.Lock()
	if len(c.files) > 0 {
		reply.Status = 1
		reply.FileName = c.files[0]
		c.files = c.files[1:]
	} else {
		reply.Status = 0
	}
	mx.Unlock()
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
// mrcoordinator通过这个来检查是否任务已完成
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if len(c.files) == 0 {
		ret = true
	}
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
	c.files = files
	c.reduceNum = nReduce
	// code end
	c.server()
	return &c
}
