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
type Args struct {
	// Map完成后的回传
	Inter    []KeyValue
	FileName string
}

type Reply struct {
	// 文件名
	FileName string
	// 文件内容
	FileContents string
	// 任务类型 1 Map,2 Reduce
	TaskType int

	// reduce任务的内容
	Key    string
	Values []string
}

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
