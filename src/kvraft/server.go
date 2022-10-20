package kvraft

import (
	"fmt"
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
	Tag   int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	maxraftstate int

	data map[string]string // K/V数据库
	ops  map[int64]chan Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if kv.isLeader() {
		DPrintf("S[%v] start Get key[%v]\n", kv.me, args.Key)
		kv.rf.Start(Op{
			Type: "Get",
			Key:  args.Key,
			Tag:  args.Tag,
		})
		kv.ops[args.Tag] = make(chan Err)
		kv.mu.Unlock()
		reply.Err = <-kv.ops[args.Tag]
		kv.mu.Lock()
		if reply.Err == OK {
			reply.Value = kv.data[args.Key]
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.isLeader() {
		DPrintf("S[%v] start %v key[%v] value[%v]\n", kv.me, args.Op, args.Key, args.Value)
		kv.rf.Start(Op{
			Type:  args.Op,
			Key:   args.Key,
			Value: args.Value,
			Tag:   args.Tag,
		})
		kv.ops[args.Tag] = make(chan Err)
		kv.mu.Unlock()
		reply.Err = <-kv.ops[args.Tag]
		kv.mu.Lock()
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()
}

// 检查自己是否为Leader,Lock使用
func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// 回复自己的某个任务,Lock使用
func (kv *KVServer) replyOp(tag int64, s Err) {
	if ch, ok := kv.ops[tag]; ok {
		ch <- s
		close(ch)
		delete(kv.ops, tag)
	}
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		// todo:exit Ch,reach Snapshot
		msg := <-kv.applyCh
		command, _ := msg.Command.(Op)
		kv.mu.Lock()
		if command.Type == "Put" {
			kv.data[command.Key] = command.Value
			kv.replyOp(command.Tag, OK)
		} else if command.Type == "Append" {
			if _, ok := kv.data[command.Key]; ok {
				kv.data[command.Key] += command.Value
				kv.replyOp(command.Tag, OK)
			} else {
				kv.replyOp(command.Tag, ErrNoKey)
			}
			fmt.Println(kv.data)
		} else {
			// Get
			if _, ok := kv.data[command.Key]; ok {
				kv.replyOp(command.Tag, OK)
			} else {
				kv.replyOp(command.Tag, ErrNoKey)
			}
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		maxraftstate: maxraftstate,
		data:         make(map[string]string),
		ops:          make(map[int64]chan Err),
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
