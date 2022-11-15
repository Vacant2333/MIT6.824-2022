package kvraft

import (
	"bytes"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"sync"
	"sync/atomic"
)

type Op struct {
	Type        string // 任务类型
	Key         string
	Value       string
	ClientTag   ClientTag // 任务的Client
	ClientIndex int       // 对应的Client的这条任务的下标
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft         // 该状态机对应的Raft
	applyCh chan raft.ApplyMsg // 从Raft发送过来的Log
	dead    int32              // 同Raft的dead
	// 3A
	kv                  map[string]string  // (持久化)Key/Value数据库
	clientLastTaskIndex map[ClientTag]int  // (持久化)每个客户端已完成的最后一个任务的下标
	doneIndex           map[int]ClientTag  // 用来检查完成的任务是否是自己发出的任务,任务Index:任务Tag
	doneCond            map[int]*sync.Cond // Client发送到该Server的任务,任务完成后通知Cond回复Client
	// 3B
	checkSnapshotCond sync.Cond // checkSnapshot协程的Cond
	persister         *raft.Persister
	maxRaftState      int // 当Raft的RaftStateSize接近该值时进行Snapshot
	lastAppliedIndex  int // 接收到的最后一条已提交的Log
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	done, isLeader, index := kv.startOp("Get", args.Key, "", args.ClientTag, args.TaskIndex)
	if done {
		// 任务已完成过
		reply.Err = OK
		reply.Value = kv.kv[args.Key]
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		kv.mu.Unlock()
		// 等待任务完成,推送至cond唤醒
		kv.doneCond[index].L.Lock()
		kv.doneCond[index].Wait()
		kv.doneCond[index].L.Unlock()
		kv.mu.Lock()
		doneTag, _ := kv.doneIndex[index]
		// 任务对应的Index已经被Apply(已完成),检查完成的任务是否是自己发布的那个
		if doneTag == args.Tag {
			value, ok := kv.kv[args.Key]
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			// 任务被ntr了:(
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	done, isLeader, index := kv.startOp(args.Op, args.Key, args.Value, args.ClientTag, args.TaskIndex)
	if done {
		// 任务已完成过
		reply.Err = OK
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		kv.mu.Unlock()
		// 等待任务完成,推送至cond唤醒
		kv.doneCond[index].L.Lock()
		kv.doneCond[index].Wait()
		kv.doneCond[index].L.Unlock()
		kv.mu.Lock()
		doneTag, _ := kv.doneIndex[index]
		if doneTag == args.Tag {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Unlock()
}

// 开始执行一条Log,Lock使用(已完成过该任务,是否为Leader,新任务的index)
func (kv *KVServer) startOp(op string, key string, value string, client ClientTag, index ClientTaskIndex) (bool, bool, int) {
	if kv.isTaskDone(client, index) {
		// 这个任务已经完成过,直接返回
		return true, false, 0
	}
	index, _, isLeader := kv.rf.Start(Op{
		Type:  op,
		Key:   key,
		Value: value,
		Tag:   opTag,
	})
	if !isLeader {
		// 我不是Leader
		return false, false, 0
	}
	if _, ok := kv.doneCond[index]; !ok {
		// 没有执行过这条Log,存入一个cond,完成该任务后通过这个cond通知所有goroutine
		kv.doneCond[index] = &sync.Cond{L: &sync.Mutex{}}
	}
	return false, true, index
}

// 持续接受来自Raft的Log
func (kv *KVServer) applier() {
	for kv.killed() == false {
		// 接受一条被Apply的Log
		msg := <-kv.applyCh
		// 解析Log中的command
		command, _ := msg.Command.(Op)
		kv.mu.Lock()
		// 检查任务是否已完成过(一个任务/Log可能会发送多次,因为前几次可能因为某种原因没有及时提交)
		firstDone := !kv.haveDoneTask(command.Tag, true)
		// 保存已提交的Log的Tag
		kv.doneIndex[msg.CommandIndex] = command.Tag
		if firstDone {
			// 如果是第一次完成该任务/Log,才保存到KV中
			if command.Type == "Put" {
				kv.kv[command.Key] = command.Value
			} else if command.Type == "Append" {
				if _, ok := kv.kv[command.Key]; ok {
					kv.kv[command.Key] += command.Value
				} else {
					kv.kv[command.Key] = command.Value
				}
			}
		}
		// 通知所有在等待该任务的goroutine
		if cond, ok := kv.doneCond[msg.CommandIndex]; ok {
			cond.Broadcast()
		}
		kv.lastAppliedIndex = msg.CommandIndex
		kv.checkSnapshotCond.Signal()
		kv.mu.Unlock()
	}
}

// 某个任务是否已完成过
//func (kv *KVServer) isTaskDone(client ClientTag, index int) bool {
//	if lastIndex, ok := kv.clientLastTaskIndex[client]; ok {
//		// Client给过任务,Index小于等于lastIndex就是已完成过该任务
//		return index <= lastIndex
//	} else {
//		// 这个Client的这个任务还没完成
//		return false
//	}
//}

// 通过ClientTag获得该Client完成的最后一条任务的下标,0则没有完成
func (kv *KVServer) getClientLastIndex(client ClientTag) int {

}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	labgob.Register(Op{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:                  me,
		rf:                  raft.Make(servers, me, persister, applyCh),
		applyCh:             applyCh,
		maxRaftState:        maxRaftState,
		kv:                  make(map[string]string),
		clientLastTaskIndex: make(map[ClientTag]int),
		doneIndex:           make(map[int]ClientTag),
		doneCond:            make(map[int]*sync.Cond),
		checkSnapshotCond:   sync.Cond{L: &sync.Mutex{}},
		persister:           persister,
	}
	// 如果是-1,则不需要进行快照
	if maxRaftState != -1 {
		go kv.checkSnapshot()
	}
	go kv.applier()
	return kv
}

// 检查是否需要Snapshot来节省日志空间
func (kv *KVServer) checkSnapshot() {
	for kv.killed() == false {
		kv.checkSnapshotCond.L.Lock()
		// 等待被唤醒
		kv.checkSnapshotCond.Wait()
		kv.checkSnapshotCond.L.Unlock()
		kv.mu.Lock()
		if float64(kv.persister.RaftStateSize()) >= float64(kv.maxRaftState)*serverSnapshotStatePercent {
			// Raft状态的大小接近阈值,发送Snapshot
			writer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(writer)
			encoder.Encode(kv.kv)
			encoder.Encode(kv.clientLastTaskIndex)
			kv.rf.Snapshot(kv.lastAppliedIndex, writer.Bytes())
			DPrintf("S[%v] snapshot(%v, %v)\n", kv.me, kv.lastAppliedIndex, len(writer.Bytes()))
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
