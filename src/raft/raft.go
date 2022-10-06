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
	"bytes"
	"fmt"
	"math/rand"
	"mit6.824/labgob"
	"sort"
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

	TickerSleepTime   = 10 * time.Millisecond  // Ticker 睡眠时间 ms
	ApplierSleepTime  = 6 * time.Millisecond   // Applier睡眠时间
	ElectionSleepTime = 5 * time.Millisecond   // 选举睡眠时间
	HeartBeatSendTime = 100 * time.Millisecond // 心跳包发送时间 ms

	PushLogsTime           = 5 * time.Millisecond  // Leader推送Log的间隔时间
	checkCommittedLogsTime = 15 * time.Millisecond // Leader更新CommitIndex的间隔时间

	ElectionTimeOutMin = 300 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutMax = 425
	//ElectionTimeOutMin = 500 // 选举超时时间(也用于检查是否需要开始选举) 区间
	//ElectionTimeOutMax = 750
	// 不改时间了
)

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
	VotedFor         int       // (持久化)当前投票给的用户
	CurrentTerm      int       // (持久化)当前任期
	peersVoteGranted []bool    // 已获得的选票
	role             int       // 角色， Follower, Candidate, Leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2A end
	// 2B start
	applyCh          chan ApplyMsg // apply的Log发送到这个Chan里
	Logs             []ApplyMsg    // (持久化)日志条目;每个条目包含了用于状态机的命令,以及领导者接收到该条目时的任期(第一个索引为1)
	commitIndex      int           // 日志中最后一条提交了的Log的下标(易失,初始为0,单调递增)
	lastAppliedIndex int           // 日志中被应用到状态机的最高一条的下标(易失,初始为0,单调递增)
	nextIndex        []int         // (for Leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int         // (for Leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	termIndex        map[int]int   // (for Leader,成为Leader后初始化)Logs中每个Term的第一条Log的Index
	// 2B end
	// 2C start
	lastNewEntryIndex int // (for Follower)最后一个新Entre的Index,用于更新commitIndex
	// 2C end
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	// 2A start
	rf.mu.Lock()
	term = rf.CurrentTerm
	isLeader = rf.role == Leader
	rf.mu.Unlock()
	// 2A end
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 保存状态,必须在Lock的情况下使用,在持久性的状态更改时就需要调用此函数
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.Logs)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 读取状态,必须在Lock的时候使用
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	var votedFor, currentTerm int
	var logs []ApplyMsg
	if decoder.Decode(&votedFor) == nil &&
		decoder.Decode(&currentTerm) == nil &&
		decoder.Decode(&logs) == nil {
		rf.VotedFor = votedFor
		rf.CurrentTerm = currentTerm
		rf.Logs = logs
	}
	fmt.Printf("s[%v] readPersist logsLen:%v Term:%v\n", rf.me, len(logs), rf.CurrentTerm)
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
	CandidateTerm         int // 候选人的任期号
	CandidateIndex        int // 候选人的Index
	CandidateLastLogIndex int // 候选人的最后日志条目的索引
	CandidateLastLogTerm  int // 候选人最后日志条目的任期
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	FollowerTerm int  // 当前任期号,以便候选人更新自己的任期号
	VoteGranted  bool // 候选人是否赢得选票
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.CandidateTerm > rf.CurrentTerm {
		//fmt.Printf("s[%v] update Term [%v]->[%v] by s[%v] when RequestVote\n", rf.me, rf.CurrentTerm, args.CandidateTerm, args.CandidateIndex)
		rf.transToFollower(args.CandidateTerm)
	}
	reply.FollowerTerm = rf.CurrentTerm
	// 是否投票给他
	if args.CandidateTerm >= rf.CurrentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateIndex) {
		// Candidate的Log至少和自己一样新,才能给他投票
		if len(rf.Logs) == 0 {
			// 自己没有Log
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm > rf.Logs[len(rf.Logs)-1].CommandTerm {
			// 如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新,在这里Candidate的最后一个Term大于Follower
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm == rf.Logs[len(rf.Logs)-1].CommandTerm && args.CandidateLastLogIndex >= len(rf.Logs) {
			// 如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
			reply.VoteGranted = true
		}
		if reply.VoteGranted {
			fmt.Printf("s[%v] vote to s[%v] Term:[%v]\n", rf.me, args.CandidateIndex, rf.CurrentTerm)
			rf.VotedFor = args.CandidateIndex
			rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
			rf.persist()
		}
	}
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
	// 2B start
	rf.mu.Lock()
	isLeader = rf.role == Leader
	term = rf.CurrentTerm
	if isLeader {
		index = len(rf.Logs) + 1
		term = rf.CurrentTerm
		log := ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		}
		rf.Logs = append(rf.Logs, log)
		fmt.Printf("L[%v] Leader get a Start request[%v], Index:[%v] Term:[%v]\n", rf.me, command, index, term)
		rf.persist()
		//fmt.Printf("me:%v match:%v next:%v logslen:%v\n", rf.me, rf.matchIndex, rf.nextIndex, len(rf.Logs))
		// (Leader)记录每个Term的第一条Log的Index
		if _, ok := rf.termIndex[term]; ok == false {
			rf.termIndex[term] = index
		}
	} else {
		fmt.Printf("s[%v] start role:%v voteFor:%v Term:%v logs:%v apply:%v\n", rf.me, rf.role, rf.VotedFor, rf.CurrentTerm, len(rf.Logs), rf.lastAppliedIndex)
	}

	rf.mu.Unlock()
	// 2B end
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 检查是否要开始领导选举,检查超时时间和角色,并且没有投票给别人
		//fmt.Printf("s[%v] ticker vote:%v role:%v term:%v\n", rf.me, rf.VotedFor, rf.role, rf.CurrentTerm)
		if rf.isHeartBeatTimeOut() && rf.VotedFor == -1 && rf.role == Follower {
			rf.mu.Unlock()
			fmt.Println(rf.me, "start election")
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(TickerSleepTime)
	}
}

// Apply那些Commit的Logs
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// commitIndex不能超过最后一条Log的Index todo:为啥会超?
		rf.commitIndex = min(rf.commitIndex, len(rf.Logs))
		if rf.commitIndex > rf.lastAppliedIndex {
			for index := rf.lastAppliedIndex + 1; index <= rf.commitIndex; index++ {
				rf.Logs[index-1].CommandValid = true
				rf.applyCh <- rf.Logs[index-1]
			}
			rf.lastAppliedIndex = rf.commitIndex
			rf.persist()
		}
		rf.mu.Unlock()
		time.Sleep(ApplierSleepTime)
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 初始化投票数据
	rf.CurrentTerm += 1
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	rf.persist()
	rf.VotedFor = rf.me
	// todo:persist votedFor
	// 选举开始时只有来自自己的选票
	for server := 0; server < len(rf.peers); server++ {
		rf.peersVoteGranted[server] = server == rf.me
	}
	rf.role = Candidate
	rf.mu.Unlock()
	// 并行收集选票
	go rf.collectVotes()
	// 检查结果
	for rf.killed() == false {
		rf.mu.Lock()
		voteCount := rf.getGrantedVotes()
		if rf.role == Follower {
			// 2.其他人成为了Leader,Candidate转为了Follower
			fmt.Printf("another is leader now,s[%v] Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			return
		} else if voteCount > len(rf.peers)/2 {
			// 1.赢得了大部分选票,成为Leader
			fmt.Printf("L[%v] is a Leader now Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.role = Leader
			// 初始化Leader需要的内容
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					// 初始化nextIndex为Leader的最后条目索引+1
					rf.nextIndex[server] = len(rf.Logs) + 1
					// 持续通过检查matchIndex给Follower发送新的Log
					go rf.pushLogsToFollower(server, rf.CurrentTerm)
				}
			}
			// 初始化每个Term的第一个Log的Index
			rf.termIndex = make(map[int]int)
			for i := 0; i < len(rf.Logs); i++ {
				if _, ok := rf.termIndex[rf.Logs[i].CommandTerm]; ok == false {
					rf.termIndex[rf.Logs[i].CommandTerm] = i + 1
				}
			}
			rf.mu.Unlock()
			// 持续发送心跳包
			go rf.sendHeartBeatsToAll()
			go rf.checkCommittedLogs()
			return
		} else if rf.isHeartBeatTimeOut() {
			// 3.选举超时,重新开始选举
			fmt.Printf("s[%v] re elect,Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			rf.startElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// 获得当前已获得的选票数量,Lock的时候使用
func (rf *Raft) getGrantedVotes() int {
	voteCount := 0
	for server := 0; server < len(rf.peers); server++ {
		if rf.peersVoteGranted[server] {
			voteCount++
		}
	}
	return voteCount
}

// 并行地向所有Peers收集选票
func (rf *Raft) collectVotes() {
	// 向某个Server要求选票
	askVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 检查投票结果
		rf.peersVoteGranted[server] = false
		if ok && reply.VoteGranted {
			rf.peersVoteGranted[server] = true
		} else if reply.FollowerTerm > rf.CurrentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.transToFollower(reply.FollowerTerm)
		}
	}
	// args保持一致
	rf.mu.Lock()
	askVoteArgs := &RequestVoteArgs{
		CandidateTerm:  rf.CurrentTerm,
		CandidateIndex: rf.me,
	}
	// 填入Leader的最后一条Log的Term和Index,给Follower校验后更新commitIndex
	if len(rf.Logs) > 0 {
		askVoteArgs.CandidateLastLogIndex = len(rf.Logs)
		askVoteArgs.CandidateLastLogTerm = rf.Logs[len(rf.Logs)-1].CommandTerm
	}
	//st := rf.CurrentTerm
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, askVoteArgs)
		}
	}
	// todo:test持续收集选票
	//for {
	//	rf.mu.Lock()
	//	if rf.role != Candidate || rf.killed() || rf.CurrentTerm != st {
	//		rf.mu.Unlock()
	//		break
	//	} else {
	//		rf.mu.Unlock()
	//	}
	//	for server := 0; server < len(rf.peers); server++ {
	//		if server != rf.me {
	//			go askVote(server, askVoteArgs)
	//		}
	//	}
	//	time.Sleep(10 * time.Millisecond)
	//}
}

// Leader,检查是否可以更新commitIndex
func (rf *Raft) checkCommittedLogs() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		// 如果存在一个满足 N > commitIndex 的 N，并且大多数的 matchIndex[i] ≥ N 成立，并且
		// log[N].term == currentTerm 成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
		match := make([]int, len(rf.peers))
		copy(match, rf.matchIndex)
		match[rf.me] = len(rf.Logs)
		sort.Ints(match)
		last := len(rf.Logs)
		for i := len(match) - 1; i >= 0; i-- {
			count := 0
			for j := len(match) - 1; j >= 0; j-- {
				if match[j] >= match[i] {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				last = match[i]
				break
			}
		}
		// Leader只能提交自己任期的Log,也只能通过这种方式顺带提交之前未提交的Log
		if last > rf.commitIndex && rf.Logs[last-1].CommandTerm == rf.CurrentTerm {
			fmt.Printf("L[%v] update commitIndex to [%v]\n", rf.me, last)
			rf.updateCommitIndex(last)
		}
		rf.mu.Unlock()
		time.Sleep(checkCommittedLogsTime)
	}
}

// 更新自己的commitIndex,用之前先检查,并且要Lock
func (rf *Raft) updateCommitIndex(commitIndex int) {
	if commitIndex > rf.commitIndex {
		fmt.Printf("s[%v] update commitIndex [%v]->[%v]\n", rf.me, rf.commitIndex, commitIndex)
		// commitIndex不能超过最后一条Log的Index todo:为啥会超?
		rf.commitIndex = min(commitIndex, len(rf.Logs))
	}
}

func (rf *Raft) pushLogsToFollower(server int, startTerm int) {
	// 如果推送失败就要立即Retry,启动时push一次加速同步
	retry := true
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader || rf.CurrentTerm != startTerm {
			// 如果不是Leader了或是任期更改了,停止推送
			fmt.Printf("s[%v] stop push entry to s[%v] role:%v term:%v->%v\n", rf.me, server, rf.role, startTerm, rf.CurrentTerm)
			rf.mu.Unlock()
			return
		}
		if len(rf.Logs) >= rf.nextIndex[server] || retry {
			// nextIndex最小要为1
			followerNextIndex := rf.nextIndex[server]
			pushLogs := make([]ApplyMsg, len(rf.Logs[followerNextIndex-1:]))
			// 必须Copy不能直接传,不然会导致Race
			copy(pushLogs, rf.Logs[followerNextIndex-1:])
			pushLastIndex := len(rf.Logs)
			pushOk, pushReply := rf.sendAppendEntries(pushLogs, followerNextIndex, server)
			if pushOk == false {
				// 推送请求失败,可能是超时,丢包,或者Leader状态改变
				retry = true
				rf.mu.Unlock()
				continue
			} else if pushReply.Success {
				// 检查推送完成后是否为最新,不为则继续Push,更新Follower的nextIndex,matchIndex
				retry = len(rf.Logs) != pushLastIndex
				rf.nextIndex[server] = pushLastIndex + 1
				rf.matchIndex[server] = pushLastIndex
				fmt.Printf("L[%v] push log to F[%v] success,Leader LogsLen:[%v] pushLast:[%v] pushLen:[%v]\n", rf.me, server, len(rf.Logs), pushLastIndex, len(pushLogs))
			} else if pushReply.FollowerTerm > rf.CurrentTerm {
				// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
				rf.transToFollower(pushReply.FollowerTerm)
				rf.mu.Unlock()
				return
			} else {
				// 校验失败但是请求成功,减少nextIndex且重试
				retry = true
				old := rf.nextIndex[server]
				if pushReply.ConflictTerm != -1 {
					if index, ok := rf.termIndex[pushReply.ConflictTerm]; ok {
						// Leader有对应Term的Log,nextIndex设置为Leader中对应Term的最后一条的下一条
						for ; index <= len(rf.Logs); index++ {
							if rf.Logs[index-1].CommandTerm != pushReply.ConflictTerm {
								break
							}
						}
						rf.nextIndex[server] = index
					} else {
						// Leader没有对应Term的Log
						rf.nextIndex[server] = pushReply.ConflictIndex
					}
				} else {
					// Follower的日志太短了
					rf.nextIndex[server] = pushReply.ConflictIndex
				}
				fmt.Printf("s[%v] nextIndex:[%v]->[%v]\n", server, old, rf.nextIndex[server])
			}
			// 防止nextIndex被设为0,最低为1
			rf.nextIndex[server] = max(rf.nextIndex[server], 1)
		}
		rf.mu.Unlock()
		time.Sleep(PushLogsTime)
	}
}

// 给单个Follower推送新的Logs,如果nextIndex是1的话Follower无需Check,Lock的时候使用
func (rf *Raft) sendAppendEntries(logs []ApplyMsg, nextIndex int, server int) (bool, *AppendEntriesReply) {
	args := &AppendEnTriesArgs{
		LeaderTerm:   rf.CurrentTerm,
		LeaderIndex:  rf.me,
		Logs:         logs,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	// 如果新条目不是第一条日志,填入上一条的Index和Term
	if nextIndex > 1 {
		args.PrevLogIndex, args.PrevLogTerm = nextIndex-1, rf.Logs[nextIndex-2].CommandTerm
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	// 检查Leader状态是否改变,改变的话此条RPC作废
	if args.LeaderTerm != rf.CurrentTerm || rf.nextIndex[server] != nextIndex || rf.role != Leader {
		ok = false
	}
	return ok, reply
}

// 持续发送心跳包给所有人
func (rf *Raft) sendHeartBeatsToAll() {
	// func: 发送单个心跳包给某服务器
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) bool {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		if reply.FollowerTerm > rf.CurrentTerm {
			rf.transToFollower(reply.FollowerTerm)
		}
		return ok && reply.Success
	}
	// 发送心跳包,直到自己不为Leader
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果现在不是Leader,停止发送心跳包
			rf.mu.Unlock()
			return
		}
		// 每次心跳包的args保持一致
		heartBeatArgs := &AppendEnTriesArgs{
			LeaderTerm:   rf.CurrentTerm,
			LeaderIndex:  rf.me,
			LeaderCommit: rf.commitIndex,
		}
		// 如果Leader有Log,给Prev参数赋值
		if len(rf.Logs) > 0 {
			heartBeatArgs.PrevLogIndex = len(rf.Logs)
			heartBeatArgs.PrevLogTerm = rf.Logs[len(rf.Logs)-1].CommandTerm
		}
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

// 更新状态为Follower,传入Term,必须在外部Lock的时候使用!
func (rf *Raft) transToFollower(newTerm int) {
	rf.role = Follower
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.persist()
}

// AppendEnTriesArgs 心跳/追加包
type AppendEnTriesArgs struct {
	LeaderTerm   int        // Leader的 term
	LeaderIndex  int        // Leader的 index
	PrevLogIndex int        // 新日志条目之前的日志的索引
	PrevLogTerm  int        // 新日志之前的日志的任期
	Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	LeaderCommit int        // Leader已提交的最高的日志项目的索引
}

type AppendEntriesReply struct {
	FollowerTerm  int  // Follower的Term,给Leader更新自己的Term
	Success       bool // 是否推送成功
	ConflictIndex int  // 加速同步
	ConflictTerm  int
}

// AppendEntries Follower接收Leader的追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEnTriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.CurrentTerm
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.LeaderTerm > rf.CurrentTerm {
		rf.transToFollower(args.LeaderTerm)
	} else if args.LeaderTerm < rf.CurrentTerm {
		// 不正常的包,Leader的Term小于Follower的Term
		reply.Success = false
		return
	}
	// 检查没有问题,不管是不是心跳包,更新选举超时时间
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	rf.VotedFor = -1
	rf.role = Follower
	// Follower接受到的Logs状态设为False
	for index := 0; index < len(args.Logs); index++ {
		args.Logs[index].CommandValid = false
	}
	if args.PrevLogIndex == 0 {
		// 如果是第一条Log不校验
		rf.Logs = args.Logs
		reply.Success = true
		if len(args.Logs) > 0 && args.Logs[len(args.Logs)-1].CommandIndex >= rf.commitIndex {
			rf.lastNewEntryIndex = args.Logs[len(args.Logs)-1].CommandIndex
		}
	} else if args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].CommandTerm == args.PrevLogTerm {
		// 校验正常,逐条追加,index为在Follower的Logs中的下一个Log的Index
		for _, log := range args.Logs {
			index := log.CommandIndex
			if index <= len(rf.Logs) {
				// 存在这条Log,如果Term相同则不做处理
				if rf.Logs[index-1].CommandTerm != log.CommandTerm {
					rf.Logs = rf.Logs[:index-1]
					rf.Logs = append(rf.Logs, log)
				}
			} else {
				// 不存在,直接追加
				rf.Logs = append(rf.Logs, log)
			}
		}
		reply.Success = true
		if len(args.Logs) > 0 && args.Logs[len(args.Logs)-1].CommandIndex >= rf.commitIndex {
			rf.lastNewEntryIndex = args.Logs[len(args.Logs)-1].CommandIndex
		}
	} else if args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].CommandTerm != args.PrevLogTerm {
		// 发生冲突,索引相同任期不同,删除从PrevLogIndex开始之后的所有Log
		reply.ConflictTerm = rf.Logs[args.PrevLogIndex-1].CommandTerm
		for i := 0; i < len(rf.Logs); i++ {
			if rf.Logs[i].CommandTerm == reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		rf.Logs = rf.Logs[:args.PrevLogIndex-1]
		reply.Success = false
		rf.persist()
		return
	} else {
		// 校验失败.Follower在PreLogIndex的位置没有条目
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.Logs)
		reply.Success = false
		return
	}
	//fmt.Printf("s[%v] lastNew:%v len:%v commit:%v term:%v\n", rf.me, rf.lastNewEntryIndex, len(rf.Logs), rf.commitIndex, rf.CurrentTerm)
	rf.persist()
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, rf.lastNewEntryIndex))
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
	rf.VotedFor = -1
	rf.peersVoteGranted = make([]bool, len(peers))
	rf.CurrentTerm = 0
	rf.role = Follower
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	// 2A end
	// 2B start
	rf.applyCh = applyCh
	rf.Logs = make([]ApplyMsg, 0)
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	// 2B end
	// 2C start
	//rf.lastNewEntryIndex = 0
	// 2C end
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
