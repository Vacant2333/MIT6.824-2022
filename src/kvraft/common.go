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

	clientDoTaskTimeOut   = 500 * time.Millisecond
	clientDoTaskSleepTime = 10 * time.Millisecond
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

type task struct {
	index    int
	op       string
	key      string
	value    string
	taskTag  tag
	resultCh chan string
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func nRand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
