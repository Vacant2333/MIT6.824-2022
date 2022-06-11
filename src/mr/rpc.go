package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Args Add your RPC definitions here.

type FuckArgs struct {
	WorkerID int
}

type FuckReply struct {
	// 任务类型:[1] Map任务, [2] Reduce任务
	TaskType int
	// Reduce任务数量
	ReduceCount int
	// Map任务文件名
	MapName string
	// Map任务ID
	MapID int
	// Reduce任务ID
	ReduceID int
	// Reduce任务用的,map任务数量
	MapTaskCount int
	// 是否退出Worker
	Exit bool
}

// TaskDoneArgs 任务完成结构
type TaskDoneArgs struct {
	// 任务类型:[1] Map任务, [2] Reduce任务
	TaskType int
	// Map任务文件名
	MapName string
	// Reduce任务ID
	ReduceID int
}

// WorkerExitArgs worker退出 请求
type WorkerExitArgs struct {
	WorkerID int
}

// None 空结构,用来占位
type None struct{}

// 中间文件格式 mr-MapID-ReduceID
var interFileName = "mr-%v-%v"

// 输出文件格式 mr-out-ReduceID
var outFileName = "mr-out-%v"

// code end

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
