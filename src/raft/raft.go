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
	CommandValid bool // 如果是Command Msg,则为True
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool   // 如果是Snapshot Msg,则为True
	Snapshot      []byte // Snapshot的内容(KV)
	SnapshotTerm  int    // Snapshot的最后一条Log的Term
	SnapshotIndex int    // Snapshot的最后一条Log的Index
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
	votedFor         int       // (持久化)当前投票给的用户
	currentTerm      int       // (持久化)当前任期
	peersVoteGranted []bool    // 已获得的选票
	role             int       // 角色， follower, candidate, leader
	heartBeatTimeOut time.Time // 上一次收到心跳包的时间+随机选举超时时间(在收到心跳包后再次随机一个)
	// 2B
	applyCh          chan ApplyMsg // apply的Log发送到这个Chan里
	logs             []ApplyMsg    // (持久化)日志条目;每个条目包含了用于状态机的命令,以及Leader接收到该条目时的任期(第一个索引为1)
	commitIndex      int           // 日志中最后一条提交了的Log的下标(易失,初始为0,单调递增)
	lastAppliedIndex int           // 日志中被应用到状态机的最高一条的下标(易失,初始为0,单调递增)
	nextIndex        []int         // (for leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int         // (for leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	// 2C
	lastNewEntryIndex int // (for follower)当前任期最后一个新Log的Index,用于更新commitIndex
	// 2D
	snapshotLastIndex int       // (通过snapshotData持久化)Raft在Snapshot的最后一条Log
	snapshotData      []byte    // (持久化)Snapshot的内容
	applierCond       sync.Cond // 当commitIndex更新时,唤醒applier
	checkCommitCond   sync.Cond // Leader的matchIndex变更时,唤醒checkCommitIndex
}

const (
	Debug = false

	follower  = 1
	candidate = 2
	leader    = 3

	tickerSleepTime    = 60 * time.Millisecond  // Ticker睡眠时间
	electionSleepTime  = 25 * time.Millisecond  // 选举检查结果的睡眠时间
	heartBeatSendTime  = 130 * time.Millisecond // 心跳包发送间隔
	pushLogsSleepTime  = 75 * time.Millisecond  // Leader推送Log的间隔
	condWakerSleepTime = 600 * time.Millisecond // 唤醒所有Cond的间隔

	electionTimeOutMin = 300 // 选举超时时间(也用于检查是否需要开始选举) 区间
	electionTimeOutMax = 400
)

// 检查心跳是否超时
func (rf *Raft) isHeartBeatTimeOut() bool {
	return rf.heartBeatTimeOut.Before(time.Now())
}

// 获得Logs总长度
func (rf *Raft) getLogsLen() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.getLog(-1).CommandIndex
}

// 获得一条Log,-1为最后一条
func (rf *Raft) getLog(index int) *ApplyMsg {
	if index == -1 {
		return &rf.logs[len(rf.logs)-1]
	} else if rf.snapshotLastIndex != 0 {
		return &rf.logs[index-rf.snapshotLastIndex]
	} else {
		return &rf.logs[index-1]
	}
}

// 更新任期并转为Follower,Lock的时候使用,新的任期必须大于当前任期,Lock使用
func (rf *Raft) increaseTerm(term int) {
	// VotedFor和lastNewEntry是针对当前任期的值,任期改变就需要清空
	rf.lastNewEntryIndex = 0
	rf.votedFor = -1
	rf.role = follower
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.role == leader
	rf.mu.Unlock()
	return term, isLeader
}

// 保存状态,Lock的情况下使用,在持久性的状态更改时就需要调用此函数
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	if encoder.Encode(rf.votedFor) == nil &&
		encoder.Encode(rf.currentTerm) == nil &&
		encoder.Encode(rf.logs) == nil {
		rf.persister.SaveStateAndSnapshot(writer.Bytes(), rf.snapshotData)
	}
}

// 读取状态(仅在初始化时使用)
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
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = logs
		// 从persister读取Snapshot
		SnapshotData := rf.persister.ReadSnapshot()
		if len(SnapshotData) > 0 {
			rf.snapshotData = SnapshotData
			rf.snapshotLastIndex = rf.logs[0].CommandIndex
			rf.updateCommitIndex(rf.snapshotLastIndex)
		}
		DPrintf("s[%v] readPersist logsLen:[%v] Term:[%v] snapshotLastIndex:[%v]\n", rf.me, rf.getLogsLen(), rf.currentTerm, rf.snapshotLastIndex)
	}
}

// 更新自己的commitIndex,要保证commitIndex > rf.commitIndex,并且Lock
func (rf *Raft) updateCommitIndex(commitIndex int) {
	if commitIndex > rf.commitIndex {
		DPrintf("s[%v] update commitIndex [%v]->[%v] logsLen:[%v] lastApplied[%v]\n", rf.me, rf.commitIndex, commitIndex, rf.getLogsLen(), rf.lastAppliedIndex)
		rf.commitIndex = commitIndex
		// commitIndex更改,唤醒applier
		rf.applierCond.Signal()
	}
}

// Snapshot 只会被上层Server调用,也就是主动快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if index > rf.snapshotLastIndex {
		// 丢弃Index之前的所有Log,[1, Index]的Log的数据都集合在了Snapshot里
		if rf.snapshotLastIndex == 0 {
			rf.logs = rf.logs[index-1:]
		} else {
			rf.logs = rf.logs[index-rf.snapshotLastIndex:]
		}
		rf.snapshotData = snapshot
		rf.snapshotLastIndex = index
		rf.persist()
		DPrintf("s[%v] snapshot Index:[%v] logsLen:[%v] snapshotLastIndex:[%v] nowSize[%v]\n", rf.me, index, rf.getLogsLen(), rf.snapshotLastIndex, rf.persister.RaftStateSize())
	}
	rf.mu.Unlock()
}

// leader,发送Snapshot给Follower,Lock使用
func (rf *Raft) sendInstallSnapshot(server int) {
	args := &InstallSnapshotArgs{
		LeaderTerm:       rf.currentTerm,
		LastIncludeIndex: rf.snapshotLastIndex,
		LastIncludeTerm:  rf.logs[0].CommandTerm,
		SnapshotData:     rf.snapshotData,
	}
	reply := &InstallSnapshotReply{}
	DPrintf("L[%v] try send Snapshot to s[%v] snapshotLastIndex:[%v]\n", rf.me, server, rf.snapshotLastIndex)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	if reply.FollowerTerm > rf.currentTerm {
		// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(reply.FollowerTerm)
		ok = false
	}
	if args.LeaderTerm != rf.currentTerm || rf.snapshotLastIndex != args.LastIncludeIndex {
		// 状态变更,请求作废
		ok = false
	}
	if ok {
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1
		// matchIndex状态改变,唤醒checkCommit
		rf.checkCommitCond.Signal()
	}
}

// CondInstallSnapshot Server询问Raft是否要切换到这个Snapshot,只有在InstallSnapshot被调用之后才会调用该函数
// 只有该函数返回True,Raft和Server才会保存该快照,否则两者都不保存
func (rf *Raft) CondInstallSnapshot(LastIncludeTerm int, LastIncludeIndex int, SnapshotData []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isAvailableSnapshot(LastIncludeIndex) {
		// 清空Follower的Logs,存入第一条(也就是Snapshot的最后一条)Log
		if rf.getLogsLen() >= LastIncludeIndex {
			// Follower有比Snapshot多的Log,不能清空,丢弃Index之前的所有Log
			if rf.snapshotLastIndex == 0 {
				rf.logs = rf.logs[LastIncludeIndex-1:]
			} else {
				rf.logs = rf.logs[LastIncludeIndex-rf.snapshotLastIndex:]
			}
		} else {
			rf.logs = make([]ApplyMsg, 1)
		}
		// 不管如何都要重置Snapshot对应的那条Log,即使Follower的Log比Snapshot多
		// 这条Log也有可能是错误的,Follower要保证这条Log的绝对正确
		rf.logs[0] = ApplyMsg{
			CommandIndex: LastIncludeIndex,
			CommandTerm:  LastIncludeTerm,
		}
		rf.snapshotData = SnapshotData
		rf.snapshotLastIndex = LastIncludeIndex
		rf.persist()
		return true
	}
	return false
}

// InstallSnapshot 只会被Leader调用,也就是被动快照,因为当前自身的Logs过于老旧才会被Leader调用
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm > rf.currentTerm {
		// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.LeaderTerm)
	}
	reply.FollowerTerm = rf.currentTerm
	if rf.currentTerm <= args.LeaderTerm && rf.isAvailableSnapshot(args.LastIncludeIndex) {
		go func() {
			// 发送该Snapshot到Server(状态机),使用goroutine避免被阻塞
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.SnapshotData,
				SnapshotTerm:  args.LastIncludeTerm,
				SnapshotIndex: args.LastIncludeIndex,
			}
		}()
	}
}

// 是否为可用的Snapshot,要比自己的Snapshot更新并且可以被安装(不能有未提交的Log)
func (rf *Raft) isAvailableSnapshot(lastIncludeIndex int) bool {
	return lastIncludeIndex > rf.commitIndex && rf.lastAppliedIndex == rf.commitIndex && lastIncludeIndex > rf.snapshotLastIndex
}

// RequestVote Follower,给Candidate投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateTerm > rf.currentTerm {
		// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.CandidateTerm)
	}
	reply.FollowerTerm = rf.currentTerm
	// 是否投票给他,要么自己在本任期没有投过票,要么本任期投票的对象是该Candidate
	if args.CandidateTerm >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateIndex) {
		// Candidate的Log至少和自己一样新,才能给他投票
		if rf.getLogsLen() == 0 {
			// 自己没有Log
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm > rf.getLog(-1).CommandTerm {
			// 两份日志最后的条目的任期号不同,那么任期号大的日志更加新,在这里Candidate的最后一个Term大于Follower
			reply.VoteGranted = true
		} else if args.CandidateLastLogTerm == rf.getLog(-1).CommandTerm && args.CandidateLastLogIndex >= rf.getLogsLen() {
			// 两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
			reply.VoteGranted = true
		}
		if reply.VoteGranted {
			rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
			rf.votedFor = args.CandidateIndex
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
	term := rf.currentTerm
	isLeader := rf.role == leader
	if isLeader {
		index = rf.getLogsLen() + 1
		log := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		}
		rf.logs = append(rf.logs, log)
		DPrintf("L[%v] leader get a Start request[%v], Index:[%v] Term:[%v]\n", rf.me, command, index, term)
		rf.persist()
		// 收到请求后立即推送一次Log,加速同步
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go rf.pushLog(server, rf.currentTerm)
			}
		}
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("s[%v] killed now!\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 检查是否需要开始选举
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 检查是否要开始领导选举
		if rf.isHeartBeatTimeOut() && rf.role == follower {
			DPrintf("s[%v] start a election now,Term:[%v] log:[%v]\n", rf.me, rf.currentTerm, rf.getLogsLen())
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(tickerSleepTime)
	}
}

// Cond唤醒器,防止状态改变时唤醒Cond失败(可能不在Wait中)
func (rf *Raft) condWaker() {
	for !rf.killed() {
		// 防止Cond没有收到状态改变时的Signal,仅出现于极个别情况
		rf.checkCommitCond.Signal()
		rf.applierCond.Signal()
		time.Sleep(condWakerSleepTime)
	}
}

// 提交Log到状态机,更新lastAppliedIndex为commitIndex
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastAppliedIndex {
			// 有Log没有被提交上去,开始提交
			for index := rf.lastAppliedIndex + 1; index <= rf.commitIndex && index <= rf.getLogsLen(); index++ {
				if index <= rf.snapshotLastIndex {
					// 提交的Index在snapshotLastIndex之前,直接从下一个开始提交
					index = rf.snapshotLastIndex
					rf.lastAppliedIndex = rf.snapshotLastIndex
					// 通过Snapshot更新lastAppliedIndex时,有可能导致lastAppliedIndex超过commitIndex,会使Raft无法再接受新的Snapshot
					rf.updateCommitIndex(rf.lastAppliedIndex)
					continue
				} else {
					// 正常Apply Log
					log := *rf.getLog(index)
					// 从2D开始applyCh会卡住,必须Unlock
					rf.mu.Unlock()
					rf.applyCh <- log
					rf.mu.Lock()
					rf.lastAppliedIndex = log.CommandIndex
				}
			}
			rf.persist()
		}
		// 是否Block住
		block := rf.lastAppliedIndex == rf.commitIndex
		rf.mu.Unlock()
		if block {
			// 当前lastAppliedIndex是最新的,就等待唤醒
			rf.applierCond.L.Lock()
			rf.applierCond.Wait()
			rf.applierCond.L.Unlock()
		}
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 初始化
	rf.increaseTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.role = candidate
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	// 选举开始时只获得来自自己的选票
	for server := 0; server < len(rf.peers); server++ {
		rf.peersVoteGranted[server] = server == rf.me
	}
	rf.persist()
	rf.mu.Unlock()
	// 并行地收集选票
	go rf.collectVotes()
	// 检查结果
	for !rf.killed() {
		rf.mu.Lock()
		voteCount := rf.getGrantedVotes()
		if rf.role == follower {
			// 1.其他人成为了Leader,Candidate转为了Follower
			DPrintf("C[%v] another server is a leader now, Term:[%v]\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		} else if voteCount > len(rf.peers)/2 {
			// 2.赢得了大部分选票,成为Leader
			DPrintf("L[%v] is a leader now, Term:[%v] votes:[%v]\n", rf.me, rf.currentTerm, rf.peersVoteGranted)
			rf.role = leader
			// 初始化Leader需要的内容
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					// 初始化nextIndex为Leader的最后条目索引+1
					rf.nextIndex[server] = rf.getLogsLen() + 1
					// 持续通过检查matchIndex给Follower发送新的Log
					go rf.pushLogsToFollower(server, rf.currentTerm)
				}
			}
			// 持续向所有Peers发送心跳包,以及检查是否要提交Logs
			go rf.sendHeartBeatsToAll(rf.currentTerm)
			rf.mu.Unlock()
			return
		} else if rf.isHeartBeatTimeOut() {
			// 3.选举超时,重新开始选举
			DPrintf("s[%v] election timeout, restart election now, Term:[%v]\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			rf.startElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(electionSleepTime)
	}
}

// candidate,获得当前已获得的选票数量,Lock的时候使用
func (rf *Raft) getGrantedVotes() int {
	count := 0
	for server := 0; server < len(rf.peers); server++ {
		if rf.peersVoteGranted[server] {
			count++
		}
	}
	return count
}

// candidate,并行地向所有Peers收集选票
func (rf *Raft) collectVotes() {
	// 向某个Server要求选票
	askVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		rf.mu.Lock()
		if reply.FollowerTerm > rf.currentTerm {
			// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.increaseTerm(reply.FollowerTerm)
		}
		// Term对不上(比如RPC延迟了很久)不能算入票数
		rf.peersVoteGranted[server] = ok && reply.VoteGranted && args.CandidateTerm == rf.currentTerm
		rf.mu.Unlock()
	}
	// args保持一致
	rf.mu.Lock()
	args := &RequestVoteArgs{
		CandidateTerm:  rf.currentTerm,
		CandidateIndex: rf.me,
	}
	// 填入Leader的最后一条Log的Term和Index,给Follower比较谁的日志更新
	if rf.getLogsLen() > 0 {
		args.CandidateLastLogIndex = rf.getLogsLen()
		args.CandidateLastLogTerm = rf.getLog(-1).CommandTerm
	}
	rf.mu.Unlock()
	// 并行地向除了自已以外的Server要求选票
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, args)
		}
	}
}

// leader,检查是否可以更新commitIndex,不是Leader不会唤醒这个goroutine
func (rf *Raft) checkCommittedLogs() {
	for !rf.killed() {
		// 等待matchIndex更新后唤醒自己
		rf.checkCommitCond.L.Lock()
		rf.checkCommitCond.Wait()
		rf.checkCommitCond.L.Unlock()
		rf.mu.Lock()
		// 存在一个满足N>commitIndex的N,并且大多数的matchIndex[i]≥N成立,并且log[N].term == currentTerm成立,那么令commitIndex等于这个N
		match := make([]int, len(rf.peers))
		copy(match, rf.matchIndex)
		match[rf.me] = rf.getLogsLen()
		sort.Ints(match)
		var N int
		if len(rf.peers)%2 == 0 {
			// 是偶数个Servers,(数量/2)-1的位置就是大多数
			N = match[(len(rf.peers)/2)-1]
		} else {
			// 是质数个Servers,除以二即可
			N = match[len(rf.peers)/2]
		}
		// Leader只能提交自己任期的Log,也只能通过这种方式顺带提交之前未提交的Log
		if N > rf.commitIndex && rf.getLog(N).CommandTerm == rf.currentTerm {
			DPrintf("L[%v] update commitIndex to [%v]\n", rf.me, N)
			rf.updateCommitIndex(N)
		}
		rf.mu.Unlock()
	}
}

// Leader,持续推送Logs给Follower
func (rf *Raft) pushLogsToFollower(server int, startTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if startTerm != rf.currentTerm {
			// Leader状态改变,停止推送Logs
			DPrintf("L[%v] stop push entry to F[%v], Role:[%v] Term:[%v] startTerm:[%v]\n", rf.me, server, rf.role, rf.currentTerm, startTerm)
			rf.mu.Unlock()
			return
		} else if rf.getLogsLen() >= rf.nextIndex[server] {
			go rf.pushLog(server, startTerm)
		}
		rf.mu.Unlock()
		time.Sleep(pushLogsSleepTime)
	}
}

// Leader给某个Follower推送Log
func (rf *Raft) pushLog(server int, startTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if startTerm != rf.currentTerm {
		return
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.snapshotLastIndex && rf.snapshotLastIndex != 0 {
		// Leader没有用于同步的Log,推送Snapshot,snapshotLastIndex对应的Log只能用于校验,其内容在Snapshot里
		rf.sendInstallSnapshot(server)
	} else {
		// 正常追加
		var pushLogs []ApplyMsg
		// 必须Copy,不能直接作为args,不然会导致Race
		if rf.snapshotLastIndex == 0 {
			pushLogs = make([]ApplyMsg, len(rf.logs[nextIndex-1:]))
			copy(pushLogs, rf.logs[nextIndex-1:])
		} else {
			pushLogs = make([]ApplyMsg, len(rf.logs[nextIndex-rf.snapshotLastIndex:]))
			copy(pushLogs, rf.logs[nextIndex-rf.snapshotLastIndex:])
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
			// matchIndex状态改变,唤醒checkCommit
			rf.checkCommitCond.Signal()
			DPrintf("L[%v] push log to F[%v] success,leader LogsLen:[%v] pushLast:[%v] pushLen:[%v]\n", rf.me, server, rf.getLogsLen(), pushLastIndex, len(pushLogs))
		} else {
			// 校验失败但是请求成功,减少nextIndex
			if pushReply.ConflictTerm != -1 {
				flag := false
				// Leader有ConflictTerm的Log,将nextIndex设为对应Term中的最后一条的下一条
				for _, log := range rf.logs {
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

// leader,给单个Follower推送新的Logs,nextIndex是1的话Follower无需Check,Lock的时候使用
func (rf *Raft) sendAppendEntries(logs []ApplyMsg, nextIndex int, server int) (bool, *AppendEntriesReply) {
	args := &AppendEnTriesArgs{
		LeaderTerm:   rf.currentTerm,
		Logs:         logs,
		LeaderCommit: rf.commitIndex,
	}
	// 新条目不是第一条日志,填入上一条的Index和Term
	if nextIndex > 1 {
		args.PrevLogIndex, args.PrevLogTerm = nextIndex-1, rf.getLog(nextIndex-1).CommandTerm
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if reply.FollowerTerm > rf.currentTerm {
		// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(reply.FollowerTerm)
	}
	if nextIndex != rf.nextIndex[server] || args.LeaderTerm != rf.currentTerm || rf.role != leader {
		// Leader状态改变,此条RPC作废
		ok = false
	}
	return ok, reply
}

// leader,持续发送心跳包给所有人
func (rf *Raft) sendHeartBeatsToAll(startTerm int) {
	// 发送单个心跳包给某服务器
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) {
		reply := &AppendEntriesReply{}
		rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.FollowerTerm > rf.currentTerm {
			// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.increaseTerm(reply.FollowerTerm)
		}
	}
	// 发送心跳包,直到自己不为Leader,或Term改变
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != startTerm {
			// 现在不是Leader,停止发送心跳包
			rf.mu.Unlock()
			return
		}
		args := &AppendEnTriesArgs{
			LeaderTerm:   rf.currentTerm,
			LeaderCommit: rf.commitIndex,
		}
		// Leader有Log,给Prev参数赋值
		if rf.getLogsLen() > 0 {
			args.PrevLogIndex = rf.getLogsLen()
			args.PrevLogTerm = rf.getLog(-1).CommandTerm
		}
		rf.mu.Unlock()
		// 给除了自己以外的服务器发送心跳包
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				go sendHeartBeat(server, args)
			}
		}
		time.Sleep(heartBeatSendTime)
	}
}

// AppendEntries Follower接收Leader的追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEnTriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	if args.LeaderTerm > rf.currentTerm {
		// 接收到的RPC请求或响应中,任期号T>currentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		rf.increaseTerm(args.LeaderTerm)
	} else if args.LeaderTerm < rf.currentTerm || rf.role == leader {
		// 不正常的包,Leader的Term小于Follower的Term
		reply.Success = false
		return
	}
	// 检查没有问题,不管是不是心跳包,更新选举超时时间
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	if args.PrevLogIndex < rf.snapshotLastIndex && rf.snapshotLastIndex != 0 {
		// Leader校验的Index在Follower的快照下标之前,让Leader直接从快照下标开始校验
		reply.Success = false
		reply.ConflictIndex = rf.snapshotLastIndex
		reply.ConflictTerm = rf.getLog(-1).CommandTerm
	} else if args.PrevLogIndex == 0 || (args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm == args.PrevLogTerm) {
		// 校验正常(或是第一条Log),开始逐条追加(心跳包也会走到这里,校验正常)
		reply.Success = true
		for _, log := range args.Logs {
			if log.CommandIndex <= rf.getLogsLen() {
				// Follower在这个Index有Log,Term不相同删除从此条Log开始的所有Log
				if rf.getLog(log.CommandIndex).CommandTerm != log.CommandTerm {
					if rf.snapshotLastIndex == 0 {
						rf.logs = rf.logs[:log.CommandIndex-1]
					} else {
						rf.logs = rf.logs[:log.CommandIndex-rf.snapshotLastIndex]
					}
					rf.logs = append(rf.logs, log)
				}
			} else {
				// Follower在这个Index没有Log直接追加
				rf.logs = append(rf.logs, log)
			}
		}
		rf.persist()
	} else if args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm != args.PrevLogTerm {
		// 发生冲突,Prev索引相同任期不同,返回冲突的任期和该任期的第一条Log的Index
		reply.Success = false
		reply.ConflictTerm = rf.getLog(args.PrevLogIndex).CommandTerm
		for _, log := range rf.logs {
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
		// 更新最后一条新Entry的下标,校验失败不会走到这里
		if len(args.Logs) > 0 && args.Logs[len(args.Logs)-1].CommandIndex >= rf.lastNewEntryIndex {
			rf.lastNewEntryIndex = args.Logs[len(args.Logs)-1].CommandIndex
		}
		// 更新Follower的commitIndex
		rf.updateCommitIndex(min(args.LeaderCommit, rf.lastNewEntryIndex))
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		votedFor:         -1,
		peersVoteGranted: make([]bool, len(peers)),
		role:             follower,
		heartBeatTimeOut: time.Now().Add(getRandElectionTimeOut()),
		applyCh:          applyCh,
		logs:             make([]ApplyMsg, 0),
		applierCond:      sync.Cond{L: &sync.Mutex{}},
		checkCommitCond:  sync.Cond{L: &sync.Mutex{}},
	}
	rf.readPersist(persister.ReadRaftState())
	go rf.condWaker()
	go rf.checkCommittedLogs()
	go rf.ticker()
	go rf.applier()
	return rf
}
