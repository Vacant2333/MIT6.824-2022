package kvraft

import (
	"fmt"
	"mit6.824/labrpc"
	"sync"
	"time"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	mu          sync.Mutex
	taskQueue   []task
	taskIndex   int
	clientTag   int64
	leaderIndex int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:     servers,
		taskQueue:   make([]task, 0),
		clientTag:   nRand(),
		leaderIndex: -1,
	}
	go ck.doTasks()
	return ck
}

func (ck *Clerk) doTasks() {
	for {
		ck.mu.Lock()
		if len(ck.taskQueue) > 0 {
			// 获得当前的task
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
				// 任务完成,从队列中删除这条任务,Err不一定是OK,也可能是ErrNoKey
				ck.taskQueue = ck.taskQueue[1:]
				DPrintf("C[%v] success a task:[%v]\n", ck.clientTag, currentTask)
				// 如果是Get会传回value,如果是Put/Append会传回"",让Append请求完成
				currentTask.resultCh <- value
			}
		}
		ck.mu.Unlock()
		time.Sleep(clientDoTaskSleepTime)
	}
}

// 并行的向所有Servers发送某个Task
func (ck *Clerk) askServers(op string, args interface{}) (Err, string) {
	// 所有的reply发送到该ch
	replyCh := make(chan interface{}, len(ck.servers))
	// 当前reply的server
	serverCh := make(chan int, len(ck.servers))
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
	askServer := func(server int) {
		fmt.Println("start")
		if op == "Get" {
			ck.servers[server].Call("KVServer.Get", args, replies[server])
		} else {
			ck.servers[server].Call("KVServer.PutAppend", args, replies[server])
		}
		replyCh <- replies[server]
		serverCh <- server
		fmt.Println("end")
	}
	if ck.leaderIndex != -1 {
		// 优先发给上一次保存的Leader
		go askServer(ck.leaderIndex)
	} else {
		// 没有保存leaderIndex,从所有服务器拿结果
		for server := 0; server < len(ck.servers); server++ {
			go askServer(server)
		}
	}
	// 持续检查replyCh,如果有可用的reply则直接返回
	for count := 0; count < len(ck.servers); count++ {
		var reply interface{}
		select {
		case reply = <-replyCh:
			// 拿到了reply
		case <-time.After(clientDoTaskTimeOut):
			// 任务超时
			DPrintf("C[%v] task[%v] timeout\n", ck.clientTag, args)
			break
		}
		if op == "Get" && reply != nil {
			getReply := reply.(*GetReply)
			server := <-serverCh
			if getReply.Err == OK || getReply.Err == ErrNoKey {
				ck.leaderIndex = server
				return getReply.Err, getReply.Value
			}
		} else if reply != nil {
			// Put/Append task
			putAppendReply := reply.(*PutAppendReply)
			server := <-serverCh
			if putAppendReply.Err == OK {
				ck.leaderIndex = server
				return putAppendReply.Err, ""
			}
		}
		if ck.leaderIndex != -1 {
			// 如果存了leaderIndex,只获取一次reply
			break
		}
	}
	// 没有可用的Leader或是保存的leaderIndex失效
	ck.leaderIndex = -1
	return ErrNoLeader, ""
}

func (ck *Clerk) Get(key string) string {
	resultCh := make(chan string)
	ck.mu.Lock()
	ck.taskQueue = append(ck.taskQueue, task{
		index:    ck.taskIndex + 1,
		op:       "Get",
		key:      key,
		resultCh: resultCh,
		taskTag:  tag(ck.clientTag + int64(ck.taskIndex) + 1),
	})
	ck.taskIndex++
	ck.mu.Unlock()
	return <-resultCh
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	resultCh := make(chan string)
	ck.mu.Lock()
	ck.taskQueue = append(ck.taskQueue, task{
		index:    ck.taskIndex + 1,
		op:       op,
		key:      key,
		value:    value,
		resultCh: resultCh,
		taskTag:  tag(ck.clientTag + int64(ck.taskIndex) + 1),
	})
	ck.taskIndex++
	ck.mu.Unlock()
	// 任务完成之前要Block住
	<-resultCh
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
