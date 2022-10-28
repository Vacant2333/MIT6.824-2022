package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

const (
	Debug = false

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoLeader    = "ErrNoLeader"

	clientDoTaskTimeOut     = 250 * time.Millisecond
	clientNoLeaderSleepTime = 50 * time.Millisecond

	serverSnapshotStatePercent = 0.95 // 当Raft的ReadStateSize大于该值*maxRaftState时启动Snapshot
)

type Err string

// 每个Task的唯一ID
type tag int64

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // Put/Append
	Tag   tag
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Tag tag
}

type GetReply struct {
	Err   Err
	Value string
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// 获得一个int64的随机数(Client的Tag)
func nRand() tag {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return tag(x)
}
