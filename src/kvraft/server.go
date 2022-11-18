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
	ClientTag   ClientTag       // 任务的Client
	ClientIndex ClientTaskIndex // 对应的Client的这条任务的下标
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft         // 该状态机对应的Raft
	applyCh chan raft.ApplyMsg // 从Raft发送过来的Log
	dead    int32              // 同Raft的dead
	// 3A
	kv                  map[string]string             // (持久化)Key/Value数据库
	clientLastTaskIndex map[ClientTag]ClientTaskIndex // (持久化)每个客户端已完成的最后一个任务的下标
	doneTask            map[int]int                   // 已完成的任务(用于校验完成的Index对应的任务是不是自己发布的任务)
	doneCond            map[int]*sync.Cond            // Client发送到该Server的任务,任务完成后通知Cond回复Client
	// 3B
	persister        *raft.Persister
	maxRaftState     int // 当Raft的RaftStateSize接近该值时进行Snapshot
	lastAppliedIndex int // (持久化)接收到的最后一条已提交的Log
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	done, isLeader, index, term := kv.startOp("Get", args.Key, "", args.ClientTag, args.TaskIndex)
	if done {
		// 任务已完成过
		reply.Err = OK
		reply.Value = kv.kv[args.Key]
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		cond := kv.doneCond[index]
		kv.mu.Unlock()
		cond.L.Lock()
		// 等待任务完成,推送至cond唤醒
		cond.Wait()
		cond.L.Unlock()
		kv.mu.Lock()
		// 任务对应的Index已经被Apply,检查完成的任务是否是自己发布的那个
		if term == kv.doneTask[index] {
			// 完成的任务和自己发布的任务相同
			if value, ok := kv.kv[args.Key]; ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			// 任务被其他Server处理了
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	done, isLeader, index, term := kv.startOp(args.Op, args.Key, args.Value, args.ClientTag, args.TaskIndex)
	if done {
		// 任务已完成过
		reply.Err = OK
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		cond := kv.doneCond[index]
		kv.mu.Unlock()
		cond.L.Lock()
		// 等待任务完成,推送至cond唤醒
		cond.Wait()
		cond.L.Unlock()
		kv.mu.Lock()
		if term == kv.doneTask[index] {
			// 完成的任务和自己发布的任务相同
			reply.Err = OK
		} else {
			// 任务被其他Server处理了
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Unlock()
}

// 开始执行一条Log,Lock使用(已完成过该任务,是否为Leader,新任务的index)
func (kv *KVServer) startOp(op string, key string, value string, clientTag ClientTag, clientTaskIndex ClientTaskIndex) (bool, bool, int, int) {
	if kv.getClientLastIndex(clientTag) >= clientTaskIndex {
		// 这个任务已经完成过,直接返回
		return true, false, 0, 0
	}
	// 要求Raft开始一次提交
	index, term, isLeader := kv.rf.Start(Op{
		Type:        op,
		Key:         key,
		Value:       value,
		ClientTag:   clientTag,
		ClientIndex: clientTaskIndex,
	})
	if !isLeader {
		// 不是Leader
		return false, false, 0, 0
	}
	if _, ok := kv.doneCond[index]; !ok {
		// 没有执行过这条Log,存入一个cond,完成该任务后通过这个cond通知所有goroutine
		kv.doneCond[index] = &sync.Cond{L: &sync.Mutex{}}
	}
	return false, true, index, term
}

// 持续接受来自Raft的Log
func (kv *KVServer) applier() {
	for kv.killed() == false {
		// 接受一条被Apply的Log
		msg := <-kv.applyCh
		kv.mu.Lock()
		// 更新最后Apply的Log的Index
		kv.lastAppliedIndex = msg.CommandIndex
		if !msg.SnapshotValid {
			// 如果不是Snapshot解析Log中的command
			command, _ := msg.Command.(Op)
			// 检查任务是否已完成过(一个任务/Log可能会发送多次,因为前几次可能因为某种原因没有及时提交)
			// 最后一条已完成的任务的Index必须小于当前任务才算没有完成过,因为线性一致性
			if command.Type != "Get" && kv.getClientLastIndex(command.ClientTag) < command.ClientIndex {
				// 如果是第一次完成该任务/Log,才保存到KV中
				if command.Type == "Put" {
					// Put
					kv.kv[command.Key] = command.Value
				} else {
					// Append
					if _, ok := kv.kv[command.Key]; ok {
						kv.kv[command.Key] += command.Value
					} else {
						kv.kv[command.Key] = command.Value
					}
					DPrintf("S[%v] append[%v]\n", kv.me, command.Value)
				}
				// 该任务的Index比之前存的任务Index大,更新
				kv.clientLastTaskIndex[command.ClientTag] = command.ClientIndex
			} else if command.Type != "Get" {
				DPrintf("S[%v] didnt apply [%v]", kv.me, command.Value)
			}
			if cond, ok := kv.doneCond[msg.CommandIndex]; ok {
				// 这个任务被给到过自己,保存它的Term,用来校验
				kv.doneTask[msg.CommandIndex] = msg.CommandTerm
				// 通知所有在等待该任务的goroutine
				cond.Broadcast()
				if command.Type == "Get" {
					DPrintf("s[%v] get[%v]\n", kv.me, command.Key)
				}
			}
			// 检查是否需要Snapshot
			if kv.maxRaftState != -1 && float64(kv.persister.RaftStateSize()) >= float64(kv.maxRaftState)*serverSnapshotStatePercent {
				// Raft状态的大小接近阈值,要求Raft进行Snapshot
				kv.saveSnapshot()
			}
		} else {
			// 这条Log是Snapshot
			kv.readSnapshot(msg.Snapshot, msg.SnapshotIndex)
		}
		kv.mu.Unlock()
	}
}

// 通过ClientTag获得该Client完成的最后一条任务的下标,0则没有完成
func (kv *KVServer) getClientLastIndex(client ClientTag) ClientTaskIndex {
	if last, ok := kv.clientLastTaskIndex[client]; ok {
		return last
	}
	return 0
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
		clientLastTaskIndex: make(map[ClientTag]ClientTaskIndex),
		doneTask:            make(map[int]int),
		doneCond:            make(map[int]*sync.Cond),
		persister:           persister,
	}
	go kv.applier()
	return kv
}

// 保存Snapshot
func (kv *KVServer) saveSnapshot() {
	// todo:clean
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	if encoder.Encode(kv.kv) == nil &&
		encoder.Encode(kv.clientLastTaskIndex) == nil {
		kv.rf.Snapshot(kv.lastAppliedIndex, writer.Bytes())
		DPrintf("S[%v] save snapshot(%v, %v)\n", kv.me, kv.lastAppliedIndex, len(writer.Bytes()))
	}
	kv.clean()
}

// 读取Snapshot
func (kv *KVServer) readSnapshot(data []byte, lastIndex int) {
	if lastIndex <= kv.lastAppliedIndex || data == nil || len(data) < 1 {
		return
	}
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	var kvMap map[string]string
	var clientLastTaskIndex map[ClientTag]ClientTaskIndex
	if decoder.Decode(&kvMap) == nil &&
		decoder.Decode(&clientLastTaskIndex) == nil {
		kv.kv = kvMap
		kv.clientLastTaskIndex = clientLastTaskIndex
		kv.lastAppliedIndex = lastIndex
		DPrintf("S[%v] read snapshot(%v, %v)\n", kv.me, kv.lastAppliedIndex, len(data))
	}
}

// 清理KVServer
func (kv *KVServer) clean() {
	for i := range kv.doneTask {
		if i <= kv.lastAppliedIndex {
			delete(kv.doneTask, i)
		}
	}
	for i := range kv.doneCond {
		if i <= kv.lastAppliedIndex {
			delete(kv.doneCond, i)
		}
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
