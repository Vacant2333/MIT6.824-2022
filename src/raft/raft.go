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
	"math"
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

	TickerSleepTime   = 25 * time.Millisecond  // Ticker 睡眠时间 ms
	ElectionSleepTime = 20 * time.Millisecond  // 选举睡眠时间
	HeartBeatSendTime = 120 * time.Millisecond // 心跳包发送时间 ms

	PushLogsTime           = 3 * time.Millisecond  // Leader推送Log的间隔时间
	checkCommittedLogsTime = 35 * time.Millisecond // Leader更新CommitIndex的间隔时间

	ElectionTimeOutMin = 450 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutMax = 600
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
	VotedFor         int       // (持久化)当前投票给的用户
	CurrentTerm      int       // (持久化)当前任期
	peersVoteGranted []bool    // 已获得的选票
	role             int       // 角色， Follower, Candidate, Leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2A end
	// 2B start
	applyCh          chan ApplyMsg // 已提交的Log需要发送到这个Chan里
	Logs             []ApplyMsg    // (持久化)日志条目;每个条目包含了用于状态机的命令,以及领导者接收到该条目时的任期(第一个索引为1)
	commitIndex      int           // 日志中最后一条提交了的Log的下标(易失,初始为0)
	lastAppliedIndex int           // 日志中被应用到状态机的最高一条的下标(易失,初始为0)
	nextIndex        []int         // (only Leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int         // (only Leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	termIndex        map[int]int
	// 2B end
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
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var votedFor, currentTerm int
	var logs []ApplyMsg
	if decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&logs) != nil {
		fmt.Printf("s[%v] readPersist error\n", rf.me)
	} else {
		rf.VotedFor = votedFor
		rf.CurrentTerm = currentTerm
		rf.Logs = logs
	}
	fmt.Printf("s[%v] readPersist logs:%v\n", rf.me, len(logs))
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
	// 是否投票给他
	if args.CandidateTerm >= rf.CurrentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateIndex) {
		// Candidate的Log至少和自己一样新,才能给他投票
		vote := false
		if len(rf.Logs) == 0 {
			// 自己没有Log,可以直接投
			vote = true
		} else if args.CandidateLastLogTerm > rf.Logs[len(rf.Logs)-1].CommandTerm {
			// 如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新,在这里Candidate的最后一个Term大于Follower,可以投
			vote = true
		} else if args.CandidateLastLogTerm == rf.Logs[len(rf.Logs)-1].CommandTerm && args.CandidateLastLogIndex >= len(rf.Logs) {
			// 如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
			vote = true
		}
		// 执行投票
		if vote {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateIndex
			rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
			rf.persist()
		}
	}
	reply.FollowerTerm = rf.CurrentTerm
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
		log := ApplyMsg{}
		log.Command = command
		log.CommandIndex = index
		log.CommandTerm = term
		rf.Logs = append(rf.Logs, log)
		//fmt.Printf("L[%v] Leader get a Start request[%v], index:[%v]\n", rf.me, command, index)
		rf.persist()
		if _, ok := rf.termIndex[term]; ok == false {
			rf.termIndex[term] = index
		}
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
	rf.mu.Lock()
	//fmt.Println("-----dead-----start")
	//fmt.Printf("s[%v] dead! role:[%v], Term:[%v]"+
	//	"\ncommitIndex:[%v] logsLen[%v]\nlogs:[%v]\n", rf.me, rf.role,
	//	rf.CurrentTerm, rf.commitIndex, len(rf.Logs), rf.Logs)
	//if rf.role == Leader {
	//	fmt.Println("nextIndex: ", rf.nextIndex)
	//	fmt.Println("matchIndex:", rf.matchIndex)
	//}
	//fmt.Println("-----dead-----end--")
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
		rf.mu.Lock()
		fmt.Printf("s[%v] commit:%v\n", rf.me, rf.commitIndex)
		// 检查是否要开始领导选举,检查超时时间和角色,并且没有投票给别人
		if rf.isHeartBeatTimeOut() && rf.VotedFor == -1 && rf.role == Follower {
			rf.mu.Unlock()
			rf.startElection()
			fmt.Println(rf.me, "start election")
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(TickerSleepTime)
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//fmt.Printf("s[%v] start a election now\n", rf.me)
	// 初始化投票数据
	rf.VotedFor = rf.me
	// 把投给自己设为True,其他人设为False
	for server := 0; server < len(rf.peers); server++ {
		rf.peersVoteGranted[server] = server == rf.me
	}
	rf.CurrentTerm += 1
	rf.role = Candidate
	// 选举超时时间
	electionTimeOut := time.Now().Add(getRandElectionTimeOut())
	rf.persist()
	rf.mu.Unlock()
	// 并行收集选票
	go rf.collectVotes()
	// 检查结果
	for rf.killed() == false {
		rf.mu.Lock()
		voteCount := rf.getGrantedVotes()
		if voteCount > len(rf.peers)/2 {
			// 1.赢得了大部分选票,成为Leader
			//fmt.Printf("L[%v] is a Leader now\n", rf.me)
			rf.role = Leader
			// 初始化Leader需要的内容
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					// 初始化nextIndex为Leader的最后条目索引+1
					rf.nextIndex[server] = len(rf.Logs) + 1
					// 持续通过检查matchIndex给Follower发送新的Log
					go rf.pushLogsToFollower(server)
				}
			}
			rf.termIndex = make(map[int]int)
			for i := 0; i < len(rf.Logs); i++ {
				if _, ok := rf.termIndex[rf.Logs[i].CommandTerm]; ok == false {
					rf.termIndex[rf.Logs[i].CommandTerm] = i + 1
				}
			}
			//rf.persist()
			rf.mu.Unlock()
			// 持续发送心跳包
			go rf.sendHeartBeatsToAll()
			go rf.checkCommittedLogs()
			return
		} else if rf.role == Follower {
			// 2.其他人成为了Leader,转为Follower了
			//fmt.Printf("F[%v] another server is Leader now\n", rf.me)
			rf.VotedFor = -1
			//rf.persist()
			rf.mu.Unlock()
			return
		} else if electionTimeOut.Before(time.Now()) {
			// 3.选举超时,重新开始选举
			//fmt.Printf("C[%v] election time out\n", rf.me)
			rf.VotedFor = -1
			rf.role = Follower
			//rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// 获得当前已获得的选票数量,需要Lock的时候使用
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
		} else if reply.FollowerTerm > rf.CurrentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.transToFollower(reply.FollowerTerm)
		}
	}
	rf.mu.Lock()
	// 持续并行的向所有人要求投票给自己,Args保持一致
	askVoteArgs := &RequestVoteArgs{
		CandidateTerm:         rf.CurrentTerm,
		CandidateIndex:        rf.me,
		CandidateLastLogIndex: 0,
		CandidateLastLogTerm:  0,
	}
	// 填入Leader的最后一条Log的Term和Index,给Follower校验后更新commitIndex
	if len(rf.Logs) > 0 {
		askVoteArgs.CandidateLastLogIndex = len(rf.Logs)
		askVoteArgs.CandidateLastLogTerm = rf.Logs[len(rf.Logs)-1].CommandTerm
	}
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, askVoteArgs)
		}
	}
	rf.mu.Unlock()
}

// Leader持续更新自己的已提交logs
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
		for index := 0; index < commitIndex; index++ {
			if rf.Logs[index].CommandValid == false {
				rf.Logs[index].CommandValid = true
				rf.applyCh <- rf.Logs[index]
			}
		}
		rf.commitIndex = commitIndex
		rf.persist()
	}
}

func (rf *Raft) pushLogsToFollower(server int) {
	// 如果推送失败就要立即Retry
	retry := false
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果不是Leader了 停止推送
			fmt.Printf("s[%v] is not a leader now!\n", rf.me)
			rf.mu.Unlock()
			return
		}
		if len(rf.Logs) >= rf.nextIndex[server] || retry {
			// nextIndex最小要为1
			followerNextIndex := int(math.Max(float64(rf.nextIndex[server]), 1))
			pushLogs := make([]ApplyMsg, len(rf.Logs[followerNextIndex-1:]))
			// 必须Copy不能直接传,不然会导致Race
			copy(pushLogs, rf.Logs[followerNextIndex-1:])
			pushLastIndex := len(rf.Logs)
			pushOk, pushReply := rf.sendAppendEntries(pushLogs, followerNextIndex, server)
			if pushReply.Success {
				// 成功,更新Follower的nextIndex,matchIndex
				retry = false
				rf.nextIndex[server] = pushLastIndex + 1
				rf.matchIndex[server] = pushLastIndex
				fmt.Printf("L[%v] push log to F[%v] success,Leader LogsLen:[%v] pushLast:[%v]\n", rf.me, server, len(rf.Logs), pushLastIndex)
			} else if pushReply.FollowerTerm > rf.CurrentTerm {
				// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
				rf.transToFollower(pushReply.FollowerTerm)
				rf.mu.Unlock()
				return
			} else if pushOk {
				// 推送失败但是请求成功,减少nextIndex且重试 todo:检查超时
				retry = true
				if followerNextIndex != rf.nextIndex[server] {
					fmt.Printf("s[%v] continue\n", server)
					rf.mu.Unlock()
					continue
				}
				old := rf.nextIndex[server]
				if pushReply.XTerm != -1 {
					if index, ok := rf.termIndex[pushReply.XTerm]; ok {
						// Leader有对应Term的Log
						fmt.Println(rf.termIndex, pushReply.XTerm)
						rf.nextIndex[server] = index
						fmt.Println(2, "S:", server, "nextIndex:", rf.nextIndex[server], "XTerm:", pushReply.XTerm)
					} else {
						// Leader没有对应Term的Log
						rf.nextIndex[server] = pushReply.XIndex
						fmt.Println(pushReply.XTerm, rf.termIndex)
						fmt.Println(1, "S:", server, "nextIndex:", rf.nextIndex[server], "XTerm:", pushReply.XTerm)
					}
				} else {
					// Follower的日志太短了
					rf.nextIndex[server] = pushReply.XLen + 1
					fmt.Println(3, "S:", server, "nextIndex:", rf.nextIndex[server], "XTerm:", pushReply.XTerm)
				}
				fmt.Printf("s[%v] nextIndex:[%v]->[%v]\n", server, old, rf.nextIndex[server])
			} else {
				// 推送请求失败,可能服务器挂了或者超时
				retry = true
			}
		}
		rf.mu.Unlock()
		time.Sleep(PushLogsTime)
	}
}

// 给单个Follower推送新的Logs,如果nextIndex是1的话Follower无需Check,要Lock的时候使用
func (rf *Raft) sendAppendEntries(logs []ApplyMsg, nextIndex int, server int) (bool, *AppendEntriesReply) {
	args := &AppendEnTriesArgs{
		LeaderTerm:   rf.CurrentTerm,
		LeaderIndex:  rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Logs:         logs,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	// 如果新条目不是第一条日志,填入上一条的Index和Term
	if nextIndex > 1 {
		args.PrevLogIndex, args.PrevLogTerm = nextIndex-1, rf.Logs[nextIndex-2].CommandTerm
	}
	rf.mu.Unlock()
	start := time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	fmt.Println("push time:", server, time.Now().Sub(start).Milliseconds())
	rf.mu.Lock()
	return ok, reply
}

// 持续发送心跳包给所有人
func (rf *Raft) sendHeartBeatsToAll() {
	// func: 发送单个心跳包给某服务器
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) bool {
		reply := &AppendEntriesReply{}
		start := time.Now()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		fmt.Println("heart time:", time.Now().Sub(start).Milliseconds(), ok)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		if reply.FollowerTerm > rf.CurrentTerm {
			rf.transToFollower(reply.FollowerTerm)
			//fmt.Printf("F[%v] trans to Follower because s[%v]'s Term bigger\n", rf.me, server)
		}
		return ok && reply.Success
	}
	// 发送心跳包,直到自己不为Leader
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果现在不是Leader,停止发送心跳包
			//fmt.Printf("F[%v] is not a leader now!\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// 每次心跳包的args保持一致
		heartBeatArgs := &AppendEnTriesArgs{
			LeaderTerm:   rf.CurrentTerm,
			LeaderIndex:  rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Logs:         nil,
			LeaderCommit: rf.commitIndex,
		}
		// 给Prev赋能:)
		if len(rf.Logs) > 0 {
			heartBeatArgs.PrevLogIndex = len(rf.Logs)
			heartBeatArgs.PrevLogTerm = rf.Logs[len(rf.Logs)-1].CommandTerm
			//fmt.Printf("L[%v] push heart args:%v %v\n", rf.me, heartBeatArgs, rf.Logs[len(rf.Logs)-1])
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
	FollowerTerm int  // Follower的Term,给Leader更新自己的Term
	Success      bool // 是否推送成功
	XTerm        int
	XIndex       int
	XLen         int
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
	// AppendEntries Pack的检查和处理
	if len(args.Logs) > 0 {
		// Follower接受到的Logs,已提交状态初始为False todo:commit的不要设
		for index := 0; index < len(args.Logs); index++ {
			args.Logs[index].CommandValid = false
		}
		if args.PrevLogIndex == 0 {
			// 如果是第一条Log不校验,persist在后面
			rf.Logs = args.Logs
			reply.Success = true
		} else if args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].CommandTerm == args.PrevLogTerm {
			// 校验正常 可以正常追加,persist在后面 todo:???
			rf.Logs = append(rf.Logs[:args.PrevLogIndex], args.Logs...)
			reply.Success = true
		} else if args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].CommandTerm != args.PrevLogTerm {
			// 发生冲突,索引相同任期不同,删除从PrevLogIndex开始之后的所有Log
			reply.XTerm = rf.Logs[args.PrevLogIndex-1].CommandTerm
			for i := 0; i < len(rf.Logs); i++ {
				if rf.Logs[i].CommandTerm == reply.XTerm {
					reply.XIndex = i + 1
					break
				}
			}
			reply.XLen = -1
			rf.Logs = rf.Logs[:args.PrevLogIndex-1]
			reply.Success = false
			rf.persist()
			return
		} else {
			// 校验失败.Follower在PreLogIndex的位置没有条目
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = len(rf.Logs)
			reply.Success = false
			return
		}
	}
	rf.persist()
	// commit似乎没问题
	if args.LeaderCommit > rf.commitIndex && len(rf.Logs) == args.PrevLogIndex && rf.Logs[len(rf.Logs)-1].CommandTerm == args.PrevLogTerm {
		rf.updateCommitIndex(int(math.Min(float64(args.LeaderCommit), float64(len(rf.Logs)))))
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
