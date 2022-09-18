package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"mit6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 2B: 指令的任期

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3

	TickerSleepTime   = 45 * time.Millisecond  // Ticker 睡眠时间 ms
	ElectionSleepTime = 30 * time.Millisecond  // 选举睡眠时间
	HeartBeatSendTime = 100 * time.Millisecond // 心跳包发送时间 ms

	ElectionTimeOutMin = 200 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutMax = 400
)

// 获得一个随机选举超时时间
func getRandElectionTimeOut() time.Duration {
	return time.Duration((rand.Int()%(ElectionTimeOutMax-ElectionTimeOutMin))+ElectionTimeOutMin) * time.Millisecond
}

// 检查自身心跳是否超时
func (rf *Raft) isHeartBeatTimeOut() bool {
	return rf.heartBeatTimeOut.Before(time.Now())
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// 2A start
	votedFor         int       // 当前投票给的用户
	peersVoteGranted []bool    // 已获得的选票
	currentTerm      int       // 当前任期
	role             int       // 角色， Follower, Candidate, Leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2A end
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	// 2A start
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	rf.mu.Unlock()
	// 2A end
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A start
	CandidateTerm  int // 候选人的任期号
	CandidateIndex int // 候选人的Index
	// 2A end
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A start
	FollowerTerm int  // 当前任期号,以便候选人更新自己的任期号
	VoteGranted  bool // 候选人是否赢得选票
	// 2A end
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.CandidateTerm > rf.currentTerm {
		fmt.Printf("s[%v] update Term [%v]->[%v] by s[%v] when RequestVote\n", rf.me, rf.currentTerm, args.CandidateTerm, args.CandidateIndex)
		rf.currentTerm = args.CandidateTerm
		rf.role = Follower
		rf.votedFor = -1
		rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	}
	// 是否投票给他
	if args.CandidateTerm >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateIndex
		rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	} else {
		reply.VoteGranted = false
		fmt.Printf("s[%v] reject vote to [%v],role:[%v],votedFor:[%v]\n", rf.me, args.CandidateIndex, rf.role, rf.votedFor)
	}
	reply.FollowerTerm = rf.currentTerm
	fmt.Printf("s[%v] be asked vote to [%v] reply:%v args:%v\n", rf.me, args.CandidateIndex, reply, args)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 接受一条来自客户端的Log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// 如果测试程序把这台机器kill了
	rf.mu.Lock()
	fmt.Printf("s[%v] dead now,role:[%v],Term:[%v]\n", rf.me, rf.role, rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 2A start
		// 检查是否要开始领导选举 检查超时时间和角色,并且没有投票给别人
		rf.mu.Lock()
		if rf.isHeartBeatTimeOut() && rf.votedFor == -1 && rf.role == Follower {
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(TickerSleepTime)
		// 2A end
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Printf("s[%v] start a election now\n", rf.me)
	// 初始化投票数据,每次选举前必须初始化
	rf.votedFor = rf.me
	for server := 0; server < len(rf.peers); server++ {
		// 把自己设为True
		rf.peersVoteGranted[server] = server == rf.me
	}
	rf.currentTerm += 1
	rf.role = Candidate
	// 选举超时时间
	electionTimeOut := time.Now().Add(getRandElectionTimeOut())
	rf.mu.Unlock()
	// 并行收集选票
	go rf.collectVotes()
	// 检查结果
	for rf.killed() == false {
		voteCount := rf.getGrantedVotes()
		rf.mu.Lock()
		if voteCount > len(rf.peers)/2 {
			// 1.赢得了大部分选票,成为Leader
			fmt.Printf("L[%v] is a Leader now\n", rf.me)
			rf.mu.Unlock()
			rf.role = Leader
			// 持续发送心跳包
			go rf.sendHeartBeats()
			return
		} else if rf.role == Follower {
			// 2.其他人成为了Leader,转为Follower
			fmt.Printf("F[%v] another server is Leader now\n", rf.me)
			rf.mu.Unlock()
			return
		} else if electionTimeOut.Before(time.Now()) {
			// 3.选举超时,重新开始选举
			fmt.Printf("C[%v] election time out\n", rf.me)
			rf.votedFor = -1
			rf.role = Follower
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// 获得当前已获得的选票数量
func (rf *Raft) getGrantedVotes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count := 0
	for server := 0; server < len(rf.peers); server++ {
		if rf.peersVoteGranted[server] {
			count++
		}
	}
	return count
}

// 并行地向所有Peers收集选票
func (rf *Raft) collectVotes() {
	// 询问某个Server要求选票
	askVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 检查投票结果
		rf.peersVoteGranted[server] = false
		if ok && reply.VoteGranted {
			// 如果投票给了自己
			rf.peersVoteGranted[server] = true
		} else if reply.FollowerTerm > rf.currentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.votedFor = -1
			rf.currentTerm = reply.FollowerTerm
			rf.role = Follower
		}
	}
	rf.mu.Lock()
	// 持续并行的向所有人要求投票给自己,Args保持一致
	askVoteArgs := &RequestVoteArgs{rf.currentTerm, rf.me}
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, askVoteArgs)
		}
	}
	rf.mu.Unlock()
}

// 持续发送心跳包给所有人
func (rf *Raft) sendHeartBeats() {
	// func : 发送单个心跳包
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) bool {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		if reply.FollowerTerm > rf.currentTerm {
			rf.votedFor = -1
			rf.role = Follower
			rf.currentTerm = reply.FollowerTerm
		}
		return ok && reply.Success
	}
	// 发送心跳包,直到自己不为Leader
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果现在不是Leader,停止发送心跳包
			fmt.Printf("F[%v] is not a leader now!\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// 每次心跳包的args保持一致
		heartBeatArgs := &AppendEnTriesArgs{rf.currentTerm, rf.me, nil}
		rf.mu.Unlock()
		// 给除了自己以外的服务器发送心跳包
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go sendHeartBeat(server, heartBeatArgs)
			}
		}
		time.Sleep(HeartBeatSendTime)
	}
}

// AppendEnTriesArgs 心跳/追加包
type AppendEnTriesArgs struct {
	LeaderTerm  int // Leader的 term
	LeaderIndex int // Leader的 index
	// 2B
	Logs []ApplyMsg // 追加的Log,可能有多个
}

type AppendEntriesReply struct {
	FollowerTerm int // Follower的Term,给Leader更新自己的Term
	Success      bool
}

// AppendEntries Follower接收Leader的追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEnTriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	// 如果接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.LeaderTerm > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		fmt.Printf("s[%v] trans to Follower by L[%v] when receive AppendEntries\n",
			rf.me, args.LeaderIndex)
	} else if args.LeaderTerm < rf.currentTerm {
		// 不正常的包,Leader的Term小于Follower的Term
		return
	}
	// 检查没有问题,不管是不是心跳包,更新选举超时时间
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	rf.votedFor = -1
	rf.role = Follower
	reply.Success = true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// 2A start
	rf.votedFor = -1
	rf.peersVoteGranted = make([]bool, len(peers))
	rf.currentTerm = 0
	rf.role = Follower
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	fmt.Printf("build s[%d] peer's count [%d]\n", rf.me, len(rf.peers))
	// 2A end
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
