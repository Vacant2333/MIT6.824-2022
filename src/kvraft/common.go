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
	// Client任务超时
	clientDoTaskTimeOut = 800 * time.Millisecond
	// Client没找到Leader的等待时间
	clientNoLeaderSleepTime = 65 * time.Millisecond
	// 当Raft的ReadStateSize大于(该值*maxRaftState)时开始Snapshot
	serverSnapshotStatePercent = 0.9
)

type Err string

// ClientId 每个Client的唯一Tag
type ClientId int64

// RequestId 每个Task的Index
type RequestId int64

type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // Put/Append
	ClientTag ClientId
	TaskIndex RequestId
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientTag ClientId
	TaskIndex RequestId
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
func nRand() ClientId {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	return ClientId(bigX.Int64())
}
