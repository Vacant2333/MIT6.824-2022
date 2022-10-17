package raft

import (
	"log"
	"math/rand"
	"time"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

// 获得一个随机选举超时时间
func getRandElectionTimeOut() time.Duration {
	return time.Duration((rand.Int()%(ElectionTimeOutMax-ElectionTimeOutMin))+ElectionTimeOutMin) * time.Millisecond
}
