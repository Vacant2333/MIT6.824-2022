package raft

import (
	"bytes"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEnTriesArgs struct {
	LeaderTerm   int        // Leader的Term
	PrevLogIndex int        // 新日志条目的上一个日志的索引
	PrevLogTerm  int        // 新日志的上一个日志的任期
	Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	LeaderCommit int        // Leader已提交的最高的日志项目的索引
}

type AppendEntriesReply struct {
	FollowerTerm  int  // Follower的Term,给Leader更新自己的Term
	Success       bool // 是否推送成功
	ConflictIndex int  // 冲突的条目的下标
	ConflictTerm  int  // 冲突的条目的任期
}

type RequestVoteArgs struct {
	CandidateTerm         int // Candidate的任期号
	CandidateIndex        int // Candidate的Index
	CandidateLastLogIndex int // Candidate的最后日志条目的索引
	CandidateLastLogTerm  int // Candidate最后日志条目的任期
}

type RequestVoteReply struct {
	FollowerTerm int  // 当前任期号,以便Candidate更新自己的任期号
	VoteGranted  bool // Candidate是否赢得选票
}

type InstallSnapshotArgs struct {
	LeaderTerm       int    // Leader的任期
	LastIncludeIndex int    // Snapshot内容中最后一条Log的Index
	LastIncludeTerm  int    // Snapshot内容中最后一条Log的Term
	SnapshotData     []byte // Snapshot的内容
}

type InstallSnapshotReply struct {
	FollowerTerm int // Follower的任期,用于给Leader更新自己的任期
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// 2A
	VotedFor         int       // (持久化)当前投票给的用户
	CurrentTerm      int       // (持久化)当前任期
	peersVoteGranted []bool    // 已获得的选票
	role             int       // 角色， Follower, Candidate, Leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2B
	applyCh          chan ApplyMsg // apply的Log发送到这个Chan里
	Logs             []ApplyMsg    // (持久化)日志条目;每个条目包含了用于状态机的命令,以及Leader接收到该条目时的任期(第一个索引为1)
	commitIndex      int           // 日志中最后一条提交了的Log的下标(易失,初始为0,单调递增)
	lastAppliedIndex int           // 日志中被应用到状态机的最高一条的下标(易失,初始为0,单调递增)
	nextIndex        []int         // (for Leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int         // (for Leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	// 2C
	lastNewEntryIndex int // (for Follower)当前任期最后一个新Log的Index,用于更新commitIndex
	// 2D
	X               int       // Server最后一条记录在Snapshot的Log
	SnapshotData    []byte    // Snapshot的内容
	applierCond     sync.Cond // 当commitIndex更新时,唤醒applier
	checkCommitCond sync.Cond // Leader的matchIndex变更时,唤醒checkCommitIndex
}

const (
	Debug = false

	Follower  = 1
	Candidate = 2
	Leader    = 3

	TickerSleepTime   = 60 * time.Millisecond  // Ticker 睡眠时间 ms
	ElectionSleepTime = 30 * time.Millisecond  // 选举睡眠时间
	HeartBeatSendTime = 125 * time.Millisecond // 心跳包发送时间 ms

	PushLogsSleepTime = 50 * time.Millisecond // Leader推送Log的间隔时间

	ElectionTimeOutMin = 300 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutMax = 400
)

// 检查心跳是否超时
func (rf *Raft) isHeartBeatTimeOut() bool {
	return rf.heartBeatTimeOut.Before(time.Now())
}

// 获得Logs总长度
func (rf *Raft) getLogsLen() int {
	if len(rf.Logs) == 0 {
		return 0
	}
	return rf.getLog(-1).CommandIndex
}

// 获得Log,-1为最后一条
func (rf *Raft) getLog(index int) *ApplyMsg {
	if index == -1 {
		return &rf.Logs[len(rf.Logs)-1]
	} else if rf.X != 0 {
		return &rf.Logs[index-rf.X]
	} else {
		return &rf.Logs[index-1]
	}
}

// 更新任期并转为Follower,Lock的时候使用,新的任期必须大于当前任期,Lock使用
func (rf *Raft) increaseTerm(term int) {
	// VotedFor和lastNewEntry是针对当前任期的值,如果任期改变就需要清空
	rf.lastNewEntryIndex = 0
	rf.VotedFor = -1
	rf.role = Follower
	rf.CurrentTerm = term
	rf.persist()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.CurrentTerm
	isLeader := rf.role == Leader
	rf.mu.Unlock()
	return term, isLeader
}

// 保存状态,Lock的情况下使用,在持久性的状态更改时就需要调用此函数
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.Logs)
	rf.persister.SaveStateAndSnapshot(writer.Bytes(), rf.SnapshotData)
}

// 读取状态,必须在Lock的时候使用
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
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
		// 读取Snapshot
		SnapshotData := rf.persister.ReadSnapshot()
		if len(SnapshotData) > 0 {
			rf.SnapshotData = SnapshotData
			rf.X = rf.Logs[0].CommandIndex
		}
		DPrintf("s[%v] readPersist logsLen:[%v] Term:[%v] X:[%v]\n", rf.me, len(logs), rf.CurrentTerm, rf.X)
	}
}

// 更新自己的commitIndex,要保证commitIndex > rf.commitIndex,并且Lock
func (rf *Raft) updateCommitIndex(commitIndex int) {
	DPrintf("s[%v] update commitIndex [%v]->[%v] logsLen:[%v]\n", rf.me, rf.commitIndex, commitIndex, rf.getLogsLen())
	rf.commitIndex = commitIndex
	// commitIndex更改,唤醒applier
	rf.applierCond.Signal()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if index > rf.X {
		// 丢弃Index之前的所有Log
		if rf.X == 0 {
			rf.Logs = rf.Logs[index-1:]
		} else {
			rf.Logs = rf.Logs[index-rf.X:]
		}
		rf.SnapshotData = snapshot
		rf.X = index
		rf.persist()
		DPrintf("s[%v] snapshot Index:[%v] logsLen:[%v] X:[%v]\n", rf.me, index, rf.getLogsLen(), rf.X)
	}
	rf.mu.Unlock()
}

// Leader,发送Snapshot给Follower,Lock使用
func (rf *Raft) sendInstallSnapshot(server int) bool {
	args := &InstallSnapshotArgs{
		LeaderTerm:       rf.CurrentTerm,
		LastIncludeIndex: rf.X,
		LastIncludeTerm:  rf.Logs[0].CommandTerm,
		SnapshotData:     rf.SnapshotData,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	if reply.FollowerTerm > rf.CurrentTerm {
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(reply.FollowerTerm)
		ok = false
	}
	if args.LeaderTerm != rf.CurrentTerm || rf.X != args.LastIncludeIndex {
		// 状态如果变更,请求作废
		ok = false
	}
	return ok
}

// InstallSnapshot Follower,接收Leader发来的Snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.LeaderTerm > rf.CurrentTerm {
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.LeaderTerm)
	}
	reply.FollowerTerm = rf.CurrentTerm
	if rf.CurrentTerm <= args.LeaderTerm && args.LastIncludeIndex > rf.X {
		rf.Logs = make([]ApplyMsg, 0)
		lastLog := ApplyMsg{
			CommandIndex: args.LastIncludeIndex,
			CommandTerm:  args.LastIncludeTerm,
		}
		rf.Logs = append(rf.Logs, lastLog)
		rf.SnapshotData = args.SnapshotData
		rf.X = args.LastIncludeIndex
		rf.persist()
	}
	rf.mu.Unlock()
}

// RequestVote Follower,给Candidate投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateTerm > rf.CurrentTerm {
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.CandidateTerm)
	}
	reply.FollowerTerm = rf.CurrentTerm
	// 是否投票给他,要么自己在本任期没有投过票,要么本任期投票的对象是该Candidate
	if args.CandidateTerm >= rf.CurrentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateIndex) {
		// Candidate的Log至少和自己一样新,才能给他投票
		if rf.getLogsLen() == 0 {
			// 自己没有Log
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm > rf.getLog(-1).CommandTerm {
			// 如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新,在这里Candidate的最后一个Term大于Follower
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm == rf.getLog(-1).CommandTerm && args.CandidateLastLogIndex >= rf.getLogsLen() {
			// 如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
			reply.VoteGranted = true
		}
		if reply.VoteGranted {
			rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
			rf.VotedFor = args.CandidateIndex
			rf.persist()
			DPrintf("F[%v] vote to C[%v] args:[%v] logsLen:[%v]\n", rf.me, args.CandidateIndex, args, rf.getLogsLen())
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start Leader接受一条来自客户端的Log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.CurrentTerm
	isLeader := rf.role == Leader
	if isLeader {
		index = rf.getLogsLen() + 1
		log := ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		}
		rf.Logs = append(rf.Logs, log)
		DPrintf("L[%v] Leader get a Start request[%v], Index:[%v] Term:[%v]\n", rf.me, command, index, term)
		rf.persist()
		// 收到请求后立即推送一次Log,加速同步
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go rf.pushLog(server, rf.CurrentTerm)
			}
		}
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 唤醒一次goroutine让它们退出
	rf.checkCommitCond.Signal()
	rf.applierCond.Signal()
	DPrintf("s[%v] killed now!\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 检查是否需要开始选举
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 检查是否要开始领导选举
		if rf.isHeartBeatTimeOut() && rf.role == Follower {
			DPrintf("s[%v] start a election now,Term:[%v] log:[%v]\n", rf.me, rf.CurrentTerm, rf.getLogsLen())
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
		rf.applierCond.Signal()
		time.Sleep(TickerSleepTime)
	}
}

// 提交Log到状态机,更新lastAppliedIndex为commitIndex
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// last保存最后一条被apply的Log的Index,chan在block的时候可能会改变rf.commitIndex
		if rf.commitIndex > rf.lastAppliedIndex {
			// 有Log没有被提交上去,开始提交
			for index := rf.lastAppliedIndex + 1; index <= rf.commitIndex && index <= rf.getLogsLen(); index++ {
				var log *ApplyMsg
				if index <= rf.X {
					// 如果提交的是Snapshot的内容(index在X之前)
					index = rf.X
					log = &ApplyMsg{
						SnapshotValid: true,
						Snapshot:      rf.SnapshotData,
						SnapshotTerm:  rf.Logs[0].CommandTerm,
						SnapshotIndex: rf.X,
					}
				} else {
					// 正常Apply
					log = rf.getLog(index)
					log.CommandValid = true
				}
				// 从2D开始applyCh会卡住,必须Unlock
				rf.mu.Unlock()
				rf.applyCh <- *log
				rf.mu.Lock()
				rf.lastAppliedIndex = log.CommandIndex
				if rf.role == Leader {
					DPrintf("L[%v] apply X:[%v] log[%v]:%v\n", rf.me, rf.X, index, log)
				}
			}
			rf.persist()
		}
		block := rf.lastAppliedIndex == rf.commitIndex
		rf.mu.Unlock()
		if block {
			// 如果当前lastAppliedIndex是最新的,就等待唤醒
			rf.applierCond.L.Lock()
			rf.applierCond.Wait()
			rf.applierCond.L.Unlock()
		}
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 初始化投票数据
	rf.increaseTerm(rf.CurrentTerm + 1)
	rf.VotedFor = rf.me
	rf.role = Candidate
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	// 选举开始时只有来自自己的选票
	for server := 0; server < len(rf.peers); server++ {
		rf.peersVoteGranted[server] = server == rf.me
	}
	rf.persist()
	rf.mu.Unlock()
	// 并行地收集选票
	go rf.collectVotes()
	// 检查结果
	for rf.killed() == false {
		rf.mu.Lock()
		voteCount := rf.getGrantedVotes()
		if rf.role == Follower {
			// 1.其他人成为了Leader,Candidate转为了Follower
			DPrintf("C[%v] another server is a Leader now, Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			return
		} else if voteCount > len(rf.peers)/2 {
			// 2.赢得了大部分选票,成为Leader
			DPrintf("L[%v] is a Leader now, Term:[%v] votes:[%v]\n", rf.me, rf.CurrentTerm, rf.peersVoteGranted)
			rf.role = Leader
			// 初始化Leader需要的内容
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					// 初始化nextIndex为Leader的最后条目索引+1
					rf.nextIndex[server] = rf.getLogsLen() + 1
					// 持续通过检查matchIndex给Follower发送新的Log
					go rf.pushLogsToFollower(server, rf.CurrentTerm)
				}
			}
			// 持续向所有Peers发送心跳包,以及检查是否要提交Logs
			go rf.sendHeartBeatsToAll(rf.CurrentTerm)
			rf.mu.Unlock()
			return
		} else if rf.isHeartBeatTimeOut() {
			// 3.选举超时,重新开始选举
			DPrintf("s[%v] election timeout, restart election now, Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			rf.startElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// Candidate,获得当前已获得的选票数量,Lock的时候使用
func (rf *Raft) getGrantedVotes() int {
	count := 0
	for server := 0; server < len(rf.peers); server++ {
		if rf.peersVoteGranted[server] {
			count++
		}
	}
	return count
}

// Candidate,并行地向所有Peers收集选票
func (rf *Raft) collectVotes() {
	// 向某个Server要求选票
	askVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.FollowerTerm > rf.CurrentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.increaseTerm(reply.FollowerTerm)
		}
		// 如果Term对不上(比如RPC延迟了很久)不能算入票数
		rf.peersVoteGranted[server] = ok && reply.VoteGranted && args.CandidateTerm == rf.CurrentTerm
	}
	// args保持一致
	rf.mu.Lock()
	args := &RequestVoteArgs{
		CandidateTerm:  rf.CurrentTerm,
		CandidateIndex: rf.me,
	}
	// 填入Leader的最后一条Log的Term和Index,给Follower比较谁的日志更新
	if rf.getLogsLen() > 0 {
		args.CandidateLastLogIndex = rf.getLogsLen()
		args.CandidateLastLogTerm = rf.getLog(-1).CommandTerm
	}
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, args)
		}
	}
}

// Leader,检查是否可以更新commitIndex,如果不是Leader不会唤醒这个goroutine
func (rf *Raft) checkCommittedLogs() {
	for rf.killed() == false {
		// 等待matchIndex更新后唤醒自己
		rf.checkCommitCond.L.Lock()
		rf.checkCommitCond.Wait()
		rf.checkCommitCond.L.Unlock()
		rf.mu.Lock()
		// 如果存在一个满足N>commitIndex的N,并且大多数的matchIndex[i]≥N成立,并且log[N].term == currentTerm成立,那么令commitIndex等于这个N
		match := make([]int, len(rf.peers))
		copy(match, rf.matchIndex)
		match[rf.me] = rf.getLogsLen()
		sort.Ints(match)
		var N int
		if len(rf.peers)%2 == 0 {
			// 如果是偶数个Servers,(数量/2)-1的位置就是大多数
			N = match[(len(rf.peers)/2)-1]
		} else {
			// 如果是质数个Servers,除以二即可
			N = match[len(rf.peers)/2]
		}
		// Leader只能提交自己任期的Log,也只能通过这种方式顺带提交之前未提交的Log
		if N > rf.commitIndex && rf.getLog(N).CommandTerm == rf.CurrentTerm {
			DPrintf("L[%v] update commitIndex to [%v]\n", rf.me, N)
			rf.updateCommitIndex(N)
		}
		rf.mu.Unlock()
	}
}

// Leader,持续推送Logs给Follower
func (rf *Raft) pushLogsToFollower(server int, startTerm int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if startTerm != rf.CurrentTerm {
			// 如果Leader状态改变,停止推送Logs
			DPrintf("L[%v] stop push entry to F[%v], Role:[%v] Term:[%v] startTerm:[%v]\n", rf.me, server, rf.role, rf.CurrentTerm, startTerm)
			rf.mu.Unlock()
			return
		}
		if rf.getLogsLen() >= rf.nextIndex[server] {
			rf.mu.Unlock()
			go rf.pushLog(server, startTerm)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(PushLogsSleepTime)
	}
}

func (rf *Raft) pushLog(server int, startTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if startTerm != rf.CurrentTerm {
		return
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.X && rf.X != 0 {
		// Leader没有用于同步的Log,推送Snapshot,第X条Log只能用于校验,其内容在Snapshot里
		if rf.sendInstallSnapshot(server) {
			rf.matchIndex[server] = rf.X
			rf.nextIndex[server] = rf.X + 1
			rf.checkCommitCond.Signal()
		}
		DPrintf("L[%v] push Snapshot to s[%v] X:[%v]\n", rf.me, server, rf.X)
	} else {
		// 正常追加
		var pushLogs []ApplyMsg
		// 必须Copy,不能直接作为args,不然会导致Race
		if rf.X == 0 {
			pushLogs = make([]ApplyMsg, len(rf.Logs[nextIndex-1:]))
			copy(pushLogs, rf.Logs[nextIndex-1:])
		} else {
			pushLogs = make([]ApplyMsg, len(rf.Logs[nextIndex-rf.X:]))
			copy(pushLogs, rf.Logs[nextIndex-rf.X:])
		}
		pushLastIndex := rf.getLogsLen()
		pushOk, pushReply := rf.sendAppendEntries(pushLogs, nextIndex, server)
		if pushOk == false {
			// 推送请求失败,可能是超时,丢包或者Leader状态改变
			DPrintf("L[%v] push log to F[%v] timeout or status changed!\n", rf.me, server)
		} else if pushReply.Success {
			// 更新Follower的nextIndex,matchIndex
			rf.nextIndex[server] = pushLastIndex + 1
			rf.matchIndex[server] = pushLastIndex
			rf.checkCommitCond.Signal()
			DPrintf("L[%v] push log to F[%v] success,Leader LogsLen:[%v] pushLast:[%v] pushLen:[%v]\n", rf.me, server, rf.getLogsLen(), pushLastIndex, len(pushLogs))
		} else {
			// 校验失败但是请求成功,减少nextIndex
			if pushReply.ConflictTerm != -1 {
				flag := false
				// 如果Leader有ConflictTerm的Log,将nextIndex设为对应Term中的最后一条的下一条
				for _, log := range rf.Logs {
					if !flag && log.CommandTerm == pushReply.ConflictTerm {
						flag = true
					} else if flag && log.CommandTerm != pushReply.ConflictTerm {
						// Leader有对应Term的Log
						rf.nextIndex[server] = log.CommandIndex
						break
					}
				}
				if !flag {
					// Leader没有冲突Term的Log
					rf.nextIndex[server] = pushReply.ConflictIndex
				}
			} else {
				// Follower的日志太短了
				rf.nextIndex[server] = pushReply.ConflictIndex
			}
			// 防止nextIndex被设为0,最低为1
			rf.nextIndex[server] = max(rf.nextIndex[server], 1)
			DPrintf("L[%v] change F[%v] nextIndex:[%v]->[%v]\n", rf.me, server, nextIndex, rf.nextIndex[server])
		}
	}
}

// Leader,给单个Follower推送新的Logs,如果nextIndex是1的话Follower无需Check,Lock的时候使用
func (rf *Raft) sendAppendEntries(logs []ApplyMsg, nextIndex int, server int) (bool, *AppendEntriesReply) {
	args := &AppendEnTriesArgs{
		LeaderTerm:   rf.CurrentTerm,
		Logs:         logs,
		LeaderCommit: rf.commitIndex,
	}
	// 如果新条目不是第一条日志,填入上一条的Index和Term
	if nextIndex > 1 {
		args.PrevLogIndex, args.PrevLogTerm = nextIndex-1, rf.getLog(nextIndex-1).CommandTerm
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if reply.FollowerTerm > rf.CurrentTerm {
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(reply.FollowerTerm)
	}
	if args.LeaderTerm != rf.CurrentTerm || rf.role != Leader {
		// Leader状态改变,此条RPC作废
		ok = false
	}
	return ok, reply
}

// Leader,持续发送心跳包给所有人
func (rf *Raft) sendHeartBeatsToAll(startTerm int) {
	// 发送单个心跳包给某服务器
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) {
		reply := &AppendEntriesReply{}
		rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.FollowerTerm > rf.CurrentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.increaseTerm(reply.FollowerTerm)
		}
	}
	// 发送心跳包,直到自己不为Leader,或Term改变
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.CurrentTerm != startTerm {
			// 如果现在不是Leader,停止发送心跳包
			rf.mu.Unlock()
			return
		}
		// 每次心跳包的args保持一致
		heartBeatArgs := &AppendEnTriesArgs{
			LeaderTerm:   rf.CurrentTerm,
			LeaderCommit: rf.commitIndex,
		}
		// 如果Leader有Log,给Prev参数赋值
		if rf.getLogsLen() > 0 {
			heartBeatArgs.PrevLogIndex = rf.getLogsLen()
			heartBeatArgs.PrevLogTerm = rf.getLog(-1).CommandTerm
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

// AppendEntries Follower接收Leader的追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEnTriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.CurrentTerm
	if args.LeaderTerm > rf.CurrentTerm {
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.LeaderTerm)
	} else if args.LeaderTerm < rf.CurrentTerm || rf.role == Leader {
		// 不正常的包,Leader的Term不大于Follower的Term
		reply.Success = false
		return
	}
	// 检查没有问题,不管是不是心跳包,更新选举超时时间
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	if args.PrevLogIndex < rf.X && rf.X != 0 {
		// 如果Leader校验的Index在Follower的X之前,直接要求Leader发送Snapshot
		reply.Success = false
		reply.ConflictIndex = 1
		reply.ConflictTerm = -1
	} else if args.PrevLogIndex == 0 || (args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm == args.PrevLogTerm) {
		// 校验正常(或是第一条Log),开始逐条追加(心跳包也会走到这里,如果校验正常)
		reply.Success = true
		for _, log := range args.Logs {
			if log.CommandIndex <= rf.getLogsLen() {
				// Follower在这个Index有Log,如果Term不相同删除从此条Log开始的所有Log
				if rf.getLog(log.CommandIndex).CommandTerm != log.CommandTerm {
					if rf.X == 0 {
						rf.Logs = rf.Logs[:log.CommandIndex-1]
					} else {
						rf.Logs = rf.Logs[:log.CommandIndex-rf.X]
					}
					rf.Logs = append(rf.Logs, log)
				}
			} else {
				// Follower在这个Index没有Log直接追加
				rf.Logs = append(rf.Logs, log)
			}
		}
		rf.persist()
	} else if args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm != args.PrevLogTerm {
		// 发生冲突,Prev索引相同任期不同,返回冲突的任期和该任期的第一条Log的Index
		reply.Success = false
		reply.ConflictTerm = rf.getLog(args.PrevLogIndex).CommandTerm
		for _, log := range rf.Logs {
			if log.CommandTerm == reply.ConflictTerm {
				reply.ConflictIndex = log.CommandIndex
				break
			}
		}
	} else {
		// 校验失败.Follower在PreLogIndex的位置没有条目
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLogsLen()
	}
	// 校验成功才会为True,只有这种情况能对commitIndex有操作
	if reply.Success {
		// 更新最后一条新Entry的下标,如果校验失败不会走到这里
		if len(args.Logs) > 0 && args.Logs[len(args.Logs)-1].CommandIndex >= rf.lastNewEntryIndex {
			rf.lastNewEntryIndex = args.Logs[len(args.Logs)-1].CommandIndex
		}
		// 更新Follower的commitIndex
		if args.LeaderCommit > rf.commitIndex && rf.lastNewEntryIndex > rf.commitIndex {
			rf.updateCommitIndex(min(args.LeaderCommit, rf.lastNewEntryIndex))
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		VotedFor:         -1,
		peersVoteGranted: make([]bool, len(peers)),
		role:             Follower,
		heartBeatTimeOut: time.Now().Add(getRandElectionTimeOut()),
		applyCh:          applyCh,
		Logs:             make([]ApplyMsg, 0),
		applierCond:      sync.Cond{L: &sync.Mutex{}},
		checkCommitCond:  sync.Cond{L: &sync.Mutex{}},
	}
	rf.readPersist(persister.ReadRaftState())
	go rf.checkCommittedLogs()
	go rf.ticker()
	go rf.applier()
	return rf
}
