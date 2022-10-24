package kvraft

import (
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"sync"
	"sync/atomic"
)

type Op struct {
	Type  string
	Key   string
	Value string
	Tag   tag
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	maxraftstate int

	data      map[string]string // K/V数据库
	doneTasks map[tag]bool
	doneIndex map[int]tag
	doneCond  map[int]*sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	done, isLeader, index := kv.startOp("Get", args.Key, "", args.Tag)
	if done {
		// 任务已完成过
		reply.Err = OK
		reply.Value = kv.data[args.Key]
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		cond := kv.doneCond[index]
		kv.mu.Unlock()
		// 等待任务完成,推送至cond唤醒
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
		kv.mu.Lock()
		doneTag := kv.getTaskTag(index)
		// 任务对应的Index已经被Apply(已完成),检查完成的任务是否是自己发布的那个
		if doneTag == args.Tag {
			value, ok := kv.data[args.Key]
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
	done, isLeader, index := kv.startOp(args.Op, args.Key, args.Value, args.Tag)
	if done {
		// 任务已完成过
		reply.Err = OK
	} else if !isLeader {
		// 不是Leader
		reply.Err = ErrWrongLeader
	} else {
		cond := kv.doneCond[index]
		kv.mu.Unlock()
		// 等待任务完成,推送至cond唤醒
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
		kv.mu.Lock()
		doneTag := kv.getTaskTag(index)
		if doneTag == args.Tag {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Unlock()
}

// 开始执行一条Log,Lock使用 已完成过该任务,是否为Leader,新任务的index
func (kv *KVServer) startOp(op string, key string, value string, opTag tag) (bool, bool, int) {
	if kv.haveDoneTask(opTag, false) {
		// 这个任务已经完成过一次
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

func (kv *KVServer) applier() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		command, _ := msg.Command.(Op)
		kv.mu.Lock()
		canSave := !kv.haveDoneTask(command.Tag, true)
		kv.doneIndex[msg.CommandIndex] = command.Tag
		if command.Type == "Put" && canSave {
			kv.data[command.Key] = command.Value
		} else if command.Type == "Append" && canSave {
			if _, ok := kv.data[command.Key]; ok {
				kv.data[command.Key] += command.Value
			} else {
				kv.data[command.Key] = command.Value
			}
		}
		if cond, ok := kv.doneCond[msg.CommandIndex]; ok {
			cond.Broadcast()
		}
		kv.mu.Unlock()
	}
}

// 检查是否有完成过某个任务,Lock使用
func (kv *KVServer) haveDoneTask(tag tag, save bool) bool {
	_, ok := kv.doneTasks[tag]
	if save && !ok {
		// 不存在并且save为True,存入doneTags
		kv.doneTasks[tag] = true
	}
	return ok
}

// 通过index检查任务是否完成,返回是否完成和任务的tag,Lock使用
func (kv *KVServer) getTaskTag(index int) tag {
	tag, _ := kv.doneIndex[index]
	return tag
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		maxraftstate: maxraftstate,
		data:         make(map[string]string),
		doneTasks:    make(map[tag]bool),
		doneIndex:    make(map[int]tag),
		doneCond:     make(map[int]*sync.Cond),
	}
	go kv.applier()
	return kv
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
