package kvraft

import (
	"mit6.824/labrpc"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	taskQueue []task
	taskIndex int
	clientTag int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		taskQueue: make([]task, 0),
		clientTag: nRand(),
	}
	go ck.doTasks()
	return ck
}

func (ck *Clerk) doTasks() {
	for {
		if len(ck.taskQueue) > 0 {
			// 添加task
			currentTask := ck.taskQueue[0]
			var args interface{}
			// 根据任务类型设置args
			if currentTask.op == Get {
				// Get task
				args = &GetArgs{
					Key: currentTask.key,
					Tag: currentTask.taskTag,
				}
			} else {
				// Put/Append task
				args = &PutAppendArgs{
					Key:   currentTask.key,
					Value: currentTask.value,
					Op:    currentTask.op,
					Tag:   currentTask.taskTag,
				}
			}
			ok, value := ck.askServers(currentTask.op, args)
			if ok {
				// 任务完成
				ck.taskQueue = ck.taskQueue[1:]
				if currentTask.op == Get {
					currentTask.resultCh <- value
				}
			}
		}
		time.Sleep(doTaskSleepTime)
	}
}

// 并行的向所有Servers发送某个Task
func (ck *Clerk) askServers(op string, args interface{}) (bool, string) {
	// 所有的reply发送到该ch
	replyCh := make(chan interface{})
	// 初始化reply
	replies := make([]interface{}, len(ck.servers))
	for index, _ := range replies {
		if op == Get {
			replies[index] = &GetReply{}
		} else {
			replies[index] = &PutAppendReply{}
		}
	}
	// 向某个Server提交Task
	askServer := func(server int, reply interface{}) {
		ck.servers[server].Call("KVServer."+op, args, reply)
		replyCh <- reply
	}
	// 从每个服务器拿结果
	for server := 0; server < len(ck.servers); server++ {
		go askServer(server, replies[server])
	}
	// 持续检查replyCh,如果有可用的reply则直接返回
	for server := 0; server < len(ck.servers); server++ {
		if op == Get {
			reply := (<-replyCh).(GetReply)
			if reply.Err == OK {
				return true, reply.Value
			}
		} else {
			reply := (<-replyCh).(PutAppendReply)
			if reply.Err == OK {
				return true, ""
			}
		}
	}
	// 所有Server都没有拿到需要的结果,返回失败
	return false, ""
}

func (ck *Clerk) Get(key string) string {
	resultCh := make(chan string)
	defer close(resultCh)
	ck.taskQueue = append(ck.taskQueue, task{
		index:    ck.taskIndex + 1,
		op:       Get,
		key:      key,
		resultCh: resultCh,
		taskTag:  ck.clientTag + int64(ck.taskIndex) + 1,
	})
	ck.taskIndex++
	return <-resultCh
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.taskQueue = append(ck.taskQueue, task{
		index:   ck.taskIndex + 1,
		op:      op,
		key:     key,
		value:   value,
		taskTag: ck.clientTag + int64(ck.taskIndex) + 1,
	})
	ck.taskIndex++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
