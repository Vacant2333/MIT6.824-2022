package kvraft

import (
	"mit6.824/labrpc"
	"sync"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mu        sync.Mutex
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
		ck.mu.Lock()
		if len(ck.taskQueue) > 0 {
			// 添加task
			currentTask := ck.taskQueue[0]
			DPrintf("C[%v] start a task:[%v]\n", ck.clientTag, currentTask)
			var args interface{}
			// 根据任务类型设置args
			if currentTask.op == "Get" {
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
			ck.mu.Unlock()
			err, value := ck.askServers(currentTask.op, args)
			ck.mu.Lock()
			if err != ErrNoLeader {
				// 任务完成,Err不一定是OK,也可能是ErrNoKey
				DPrintf("C[%v] success a task:[%v]\n", ck.clientTag, currentTask)
				ck.taskQueue = ck.taskQueue[1:]
				if currentTask.op == "Get" {
					currentTask.resultCh <- value
				}
			} else {
				// err == ErrNoLeader,目前没有Leader能处理任务
				DPrintf("C[%v] fail a task:[%v]\n", ck.clientTag, currentTask)
			}
		}
		ck.mu.Unlock()
		time.Sleep(doTaskSleepTime)
	}
}

// 并行的向所有Servers发送某个Task,Lock使用
func (ck *Clerk) askServers(op string, args interface{}) (Err, string) {
	// 所有的reply发送到该ch
	replyCh := make(chan interface{})
	// 初始化reply
	replies := make([]interface{}, len(ck.servers))
	for index, _ := range replies {
		if op == "Get" {
			replies[index] = &GetReply{}
		} else {
			replies[index] = &PutAppendReply{}
		}
	}
	// 向某个Server提交Task
	askServer := func(server int, reply interface{}) {
		if op == "Get" {
			ck.servers[server].Call("KVServer.Get", args, reply)
		} else {
			ck.servers[server].Call("KVServer.PutAppend", args, reply)
		}
		replyCh <- reply
	}
	// 从每个服务器拿结果
	for server := 0; server < len(ck.servers); server++ {
		go askServer(server, replies[server])
	}
	// 持续检查replyCh,如果有可用的reply则直接返回
	for count := 0; count < len(ck.servers); count++ {
		if op == "Get" {
			reply := (<-replyCh).(*GetReply)
			DPrintf("C[%v] get a reply:[%v]\n", ck.clientTag, reply)
			if reply.Err != ErrWrongLeader {
				return reply.Err, reply.Value
			}
		} else {
			reply := (<-replyCh).(*PutAppendReply)
			DPrintf("C[%v] get a reply:[%v]\n", ck.clientTag, reply)
			if reply.Err != ErrWrongLeader {
				//DPrintf("C[%v] get a reply:[%v]\n", ck.clientTag, reply)
				return reply.Err, ""
			}
		}
	}
	// 所有Server都没有拿到需要的结果,返回失败
	return ErrNoLeader, ""
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	resultCh := make(chan string)
	defer close(resultCh)
	ck.taskQueue = append(ck.taskQueue, task{
		index:    ck.taskIndex + 1,
		op:       "Get",
		key:      key,
		resultCh: resultCh,
		taskTag:  ck.clientTag + int64(ck.taskIndex) + 1,
	})
	ck.taskIndex++
	ck.mu.Unlock()
	return <-resultCh
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.taskQueue = append(ck.taskQueue, task{
		index:   ck.taskIndex + 1,
		op:      op,
		key:     key,
		value:   value,
		taskTag: ck.clientTag + int64(ck.taskIndex) + 1,
	})
	ck.taskIndex++
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
