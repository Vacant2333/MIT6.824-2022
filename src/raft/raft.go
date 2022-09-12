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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// role is follower, candidate, or leader.
const (
	Follower  = 1
	Candidate = 2
	Leader    = 3

	TickerSleepTime   = 15 * time.Millisecond  // Ticker 睡眠时间 ms
	ElectionSleepTime = 18 * time.Millisecond  // 选举睡眠时间
	HeartBeatSendTime = 100 * time.Millisecond // 心跳包发送时间 ms

	ElectionTimeOutStart = 200 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutEnd   = 500
)

// 获得一个随机选举超时时间
func getRandElectionTimeOut() time.Duration {
	return time.Duration((rand.Int()%(ElectionTimeOutEnd-ElectionTimeOutStart))+ElectionTimeOutStart) * time.Millisecond
}

// 检查心跳包是否超时(heartBeatTimeOut是上次收到心跳包的时间+一个随机选举超时时间)
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
	currentTerm      int       // 当前任期
	votedFor         int       // 当前投票给的用户
	voteCount        int       // 当前得票数量(默认为1)
	role             int       // 角色，follower, candidate, leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2A end
	// 2B start
	logs             []ApplyMsg // 日志条目;每个条目包含了用于状态机的命令,以及领导者接收到该条目时的任期(第一个索引为1)
	commitIndex      int        // (所有服务器)日志中最后一条的log的下标(易失,初始为0)
	lastAppliedIndex int        // (所有服务器)日志中被应用到状态机的最高一条的下标(易失,初始为0)
	nextIndex        []int      // (only Leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int      // (only Leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	// 2B end
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// 2A start
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	// 2A end
	return term, isleader
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
	CandidateTerm int // 候选人的任期号(自己的)
	CandidateId   int // 请求选票的候选人的ID(peer's index)
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
	// 2A start
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果sender的term大于receiver,更新receiver的term和role
	if args.CandidateTerm > rf.currentTerm {
		fmt.Printf("s[%v] trans to Follower by s[%v] when RequestVote\n", rf.me, args.CandidateId)
		rf.currentTerm = args.CandidateTerm
		rf.role = Follower
		rf.votedFor = -1
		rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	}
	// 是否投票给他
	if args.CandidateTerm >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		fmt.Printf("s[%v] reject vote for [%v],role:%v votedFor:%v\n", rf.me, args.CandidateId, rf.role, rf.votedFor)
	}
	reply.FollowerTerm = rf.currentTerm
	// 2A end
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
// 用来存一条log
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
	fmt.Printf("s[%v] dead now,role:%v\n", rf.me, rf.role)
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
	// 初始化投票数据
	rf.resetVoteData(true)
	rf.mu.Lock()
	fmt.Printf("s[%v] start the election now!\n", rf.me)
	rf.currentTerm += 1
	rf.role = Candidate
	// 选举超时时间
	electionTimeOut := time.Now().Add(getRandElectionTimeOut())
	rf.mu.Unlock()
	// 并行收集选票
	rf.collectVotes()
	// 检查结果
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.voteCount > len(rf.peers)/2 {
			// 1.赢得了大部分选票
			fmt.Printf("s[%v] is Leader now!\n", rf.me)
			rf.mu.Unlock()
			rf.resetVoteData(false)
			go rf.imLeader()
			return
		}
		if rf.role == Follower {
			// 2.其他人成为了Leader
			fmt.Printf("s[%v] another server is Leader now\n", rf.me)
			rf.mu.Unlock()
			rf.resetVoteData(false)
			return
		}
		if electionTimeOut.Before(time.Now()) {
			// 3.选举超时,重新开始选举
			fmt.Printf("s[%v] election time out!\n", rf.me)
			rf.mu.Unlock()
			go rf.startElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// 向所有peers收集选票
func (rf *Raft) collectVotes() {
	// 收集单个选票
	askVote := func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
		ok := rf.sendRequestVote(server, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 检查投票
		if ok && reply.VoteGranted {
			rf.voteCount += 1
			fmt.Printf("s[%v] get a vote from s[%v], voteCount:[%v/%v]\n", rf.me, server, rf.voteCount, len(rf.peers))
		}
	}
	rf.mu.Lock()
	// 请求投票的args需要保持一致
	args := &RequestVoteArgs{rf.currentTerm, rf.me}
	reply := make([]*RequestVoteReply, len(rf.peers))
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			reply[server] = &RequestVoteReply{}
			go askVote(server, args, reply[server])
		}
	}
}

// 成为Leader
func (rf *Raft) imLeader() {
	rf.mu.Lock()
	// 初始化Leader需要的内容
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// 初始化nextIndex,存了每台服务器的下一条log的索引(初始为Leader的最大索引+1)
	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = rf.commitIndex + 1
	}
	rf.role = Leader
	rf.mu.Unlock()
	// 持续发送心跳包
	go rf.sendHeartBeats()
}

// 持续发送心跳包给所有人
func (rf *Raft) sendHeartBeats() {
	// 发送单个心跳包
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) bool {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		return ok && reply.Success
	}

	// 发送心跳包,直到自己不为Leader
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果现在不是Leader,停止发送心跳包
			fmt.Printf("s[%v] is not a leader now!\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// 每次心跳包的args保持一致
		heartBeatArgs := &AppendEnTriesArgs{rf.currentTerm, rf.me, true}
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

// 重置投票信息
func (rf *Raft) resetVoteData(voteMe bool) {
	rf.mu.Lock()
	rf.voteCount = 0
	rf.votedFor = -1
	if voteMe {
		rf.voteCount += 1
		rf.votedFor = rf.me
	}
	rf.mu.Unlock()
}

// AppendEnTriesArgs 心跳/追加包
type AppendEnTriesArgs struct {
	LeaderTerm  int  // leader's term
	LeaderId    int  // leader's index
	IsHeartBeat bool // 心跳包
}

type AppendEntriesReply struct {
	FollowerTerm int // follower's term,for leader to update its term
	Success      bool
}

// AppendEntries follower 接收追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEnTriesArgs, reply *AppendEntriesReply) {
	// 收到了来自某个Leader的心跳包,清空投票记录
	rf.resetVoteData(false)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果sender的term大于receiver,更新receiver的term和role
	if args.LeaderTerm > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.LeaderTerm
		fmt.Printf("s[%v] trans to Follower by s[%v] when AppendEntries\n", rf.me, args.LeaderId)
	}
	reply.FollowerTerm = rf.currentTerm
	if args.IsHeartBeat {
		// 处理心跳包
		if args.LeaderTerm < rf.currentTerm {
			reply.Success = false
			return
		} else {
			reply.Success = true
			// 更新心跳包超时时间
			rf.role = Follower
			rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
			return
		}
	} else {
		// 追加条目
	}
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	fmt.Printf("build s[%d] peer's count [%d]\n", rf.me, len(rf.peers))
	// 2A end
	// 2B start
	rf.logs = make([]ApplyMsg, 0)
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	// 2B end
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
