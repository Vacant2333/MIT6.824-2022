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
	ErrFailConnect = "ErrFailConnect"

	doTaskSleepTime      = 20 * time.Millisecond
	checkOpDoneSleepTime = 10 * time.Millisecond
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	Tag int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Tag int64
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
	taskTag  int64
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
