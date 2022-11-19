package kvraft

import (
	"mit6.824/labrpc"
	"sync"
	"time"
)

type task struct {
	index    RequestId   // 对于当前Client的任务的Index
	op       string      // 任务类型
	key      string      // Get/PutAppend参数
	value    string      // PutAppend参数
	resultCh chan string // 传Get的返回值和Block住Get/PutAppend方法
}

type Clerk struct {
	servers     []*labrpc.ClientEnd
	taskMu      sync.Mutex
	taskQueue   chan task // 任务队列
	clientTag   ClientId  // Client的唯一标识
	taskIndex   RequestId // 最后一条任务的下标(包括未完成的任务)
	leaderIndex int       // 上一次成功完成任务的Leader的Index,没有的话为-1
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:     servers,
		taskQueue:   make(chan task),
		clientTag:   nRand(),
		leaderIndex: -1,
	}
	go ck.doTasks()
	return ck
}

// 持续通过ck.taskQueue接受新的任务
func (ck *Clerk) doTasks() {
	for {
		currentTask := <-ck.taskQueue
		DPrintf("C[%v] start a task:[%v]\n", ck.clientTag, currentTask)
		var args interface{}
		// 根据任务类型设置args
		if currentTask.op == "Get" {
			// Get task
			args = &GetArgs{
				Key:       currentTask.key,
				TaskIndex: currentTask.index,
				ClientTag: ck.clientTag,
			}
		} else {
			// Put/Append task
			args = &PutAppendArgs{
				Key:       currentTask.key,
				Value:     currentTask.value,
				Op:        currentTask.op,
				TaskIndex: currentTask.index,
				ClientTag: ck.clientTag,
			}
		}
		for {
			if err, value := ck.startTask(currentTask.op, args); err != ErrNoLeader {
				// 任务完成,Err不一定是OK,也可能是ErrNoKey
				DPrintf("C[%v] success a task:[%v]\n", ck.clientTag, currentTask)
				// 如果是Get会传回value,如果是Put/Append会传回"",让Append请求完成
				currentTask.resultCh <- value
				break
			}
			time.Sleep(clientNoLeaderSleepTime)
		}
	}
}

// 并行的向所有Servers发送某个Task
func (ck *Clerk) startTask(op string, args interface{}) (Err, string) {
	// 所有的Reply发送到该Ch
	replyCh := make(chan interface{}, len(ck.servers))
	// 当前Reply的Server
	serverCh := make(chan int, len(ck.servers))
	// 初始化Reply
	replies := make([]interface{}, len(ck.servers))
	for index := range replies {
		if op == "Get" {
			replies[index] = &GetReply{}
		} else {
			replies[index] = &PutAppendReply{}
		}
	}
	// 向某个Server提交Task
	askServer := func(server int) {
		if op == "Get" {
			ck.servers[server].Call("KVServer.Get", args, replies[server])
		} else {
			ck.servers[server].Call("KVServer.PutAppend", args, replies[server])
		}
		replyCh <- replies[server]
		serverCh <- server
	}
	// 会收到的Reply的数量
	replyCount := len(ck.servers)
	if ck.leaderIndex != -1 {
		// 优先发给上一次保存的Leader
		go askServer(ck.leaderIndex)
		replyCount = 1
	} else {
		// 没有保存leaderIndex,从所有服务器拿结果
		for server := 0; server < len(ck.servers); server++ {
			go askServer(server)
		}
	}
	// 持续检查replyCh,如果有可用的reply则直接返回
	timeOut := time.After(clientDoTaskTimeOut)
	for ; replyCount > 0; replyCount-- {
		var reply interface{}
		select {
		case reply = <-replyCh:
			// 拿到了reply
		case <-timeOut:
			// 任务超时
			DPrintf("C[%v] task[%v] timeout,leaderIndex[%v]\n", ck.clientTag, args, ck.leaderIndex)
			ck.leaderIndex = -1
			return ErrNoLeader, ""
		}
		server := <-serverCh
		// 如果Reply不为空则返回对应的数据给ch
		if op == "Get" && reply != nil {
			// Get
			getReply := reply.(*GetReply)
			if getReply.Err == OK || getReply.Err == ErrNoKey {
				ck.leaderIndex = server
				return getReply.Err, getReply.Value
			}
		} else if reply != nil {
			// Put/Append
			putAppendReply := reply.(*PutAppendReply)
			if putAppendReply.Err == OK {
				ck.leaderIndex = server
				return putAppendReply.Err, ""
			}
		}
	}
	// 没有可用的Leader或是保存的leaderIndex失效
	ck.leaderIndex = -1
	return ErrNoLeader, ""
}

// 添加任务,返回任务结果的chan
func (ck *Clerk) addTask(op string, key string, value string) chan string {
	resultCh := make(chan string)
	ck.taskMu.Lock()
	ck.taskQueue <- task{
		index:    ck.taskIndex + 1,
		op:       op,
		key:      key,
		value:    value,
		resultCh: resultCh,
	}
	ck.taskIndex++
	ck.taskMu.Unlock()
	return resultCh
}

func (ck *Clerk) Get(key string) string {
	return <-ck.addTask("Get", key, "")
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	<-ck.addTask(op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
