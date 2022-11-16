package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

const (
	Debug = true

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoLeader    = "ErrNoLeader"

	clientDoTaskTimeOut     = 250 * time.Millisecond
	clientNoLeaderSleepTime = 50 * time.Millisecond

	serverSnapshotStatePercent = 0.9 // 当Raft的ReadStateSize大于该值*maxRaftState时启动Snapshot
)

type Err string

// ClientTag 每个Client的唯一Tag
type ClientTag int64
type ClientTaskIndex int64

type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // Put/Append
	ClientTag ClientTag
	TaskIndex ClientTaskIndex
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientTag ClientTag
	TaskIndex ClientTaskIndex
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
func nRand() ClientTag {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	return ClientTag(bigX.Int64())
}
