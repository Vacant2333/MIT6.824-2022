package raft

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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
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
	CandidateTerm         int // 候选人的任期号
	CandidateIndex        int // 候选人的Index
	CandidateLastLogIndex int // 候选人的最后日志条目的索引
	CandidateLastLogTerm  int // 候选人最后日志条目的任期
}

type RequestVoteReply struct {
	FollowerTerm int  // 当前任期号,以便候选人更新自己的任期号
	VoteGranted  bool // 候选人是否赢得选票
}

type InstallSnapshotArgs struct {
	LeaderTerm       int // 领导人的任期
	LastIncludeIndex int
	LastIncludeTerm  int
	SnapshotData     []byte
}

type InstallSnapshotReply struct {
	FollowerTerm int
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
	Logs             []ApplyMsg    // (持久化)日志条目;每个条目包含了用于状态机的命令,以及领导者接收到该条目时的任期(第一个索引为1)
	commitIndex      int           // 日志中最后一条提交了的Log的下标(易失,初始为0,单调递增)
	lastAppliedIndex int           // 日志中被应用到状态机的最高一条的下标(易失,初始为0,单调递增)
	nextIndex        []int         // (for Leader,成为Leader后初始化)对于每台服务器,发送的下一条log的索引(初始为Leader的最大索引+1)
	matchIndex       []int         // (for Leader,成为Leader后初始化)对于每台服务器,已知的已复制到该服务器的最高索引(默认为0,单调递增)
	// 2C
	lastNewEntryIndex int // (for Follower)当前任期最后一个新Log的Index,用于更新commitIndex
	// 2D
	X            int
	SnapshotData []byte
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3

	TickerSleepTime   = 25 * time.Millisecond  // Ticker 睡眠时间 ms
	ApplierSleepTime  = 20 * time.Millisecond  // Applier睡眠时间
	ElectionSleepTime = 10 * time.Millisecond  // 选举睡眠时间
	HeartBeatSendTime = 100 * time.Millisecond // 心跳包发送时间 ms

	PushLogsTime           = 15 * time.Millisecond // Leader推送Log的间隔时间
	checkCommittedLogsTime = 25 * time.Millisecond // Leader更新CommitIndex的间隔时间

	ElectionTimeOutMin = 300 // 选举超时时间(也用于检查是否需要开始选举) 区间
	ElectionTimeOutMax = 400
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

// 获得自身Logs总长度
func (rf *Raft) getLogsLen() int {
	if len(rf.Logs) == 0 {
		return 0
	}
	return rf.getLog(-1).CommandIndex
}

// 获得某条Log,-1为最后一条
func (rf *Raft) getLog(index int) *ApplyMsg {
	if rf.X > 0 {
		//fmt.Printf("s[%v] getLog[%v] logsLen(new):[%v] X:[%v] len:[%v]\n", rf.me, index, len(rf.Logs), rf.X, rf.Logs[len(rf.Logs)-1].CommandIndex)
		//fmt.Println(rf.Logs)
	} else {
		//fmt.Printf("s[%v] getLog[%v] logsLen(new):[%v] X:[%v] len:[%v]\n", rf.me, index, len(rf.Logs), rf.X, rf.Logs[len(rf.Logs)-1].CommandIndex)
	}
	if index == -1 {
		return &rf.Logs[len(rf.Logs)-1]
	} else if rf.X != 0 {
		return &rf.Logs[index-rf.X]
	} else {
		return &rf.Logs[index-1]
	}
}

// 更新任期并转为Follower,Lock的时候使用,新的任期必须大于当前任期
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

// 保存状态,必须在Lock的情况下使用,在持久性的状态更改时就需要调用此函数
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
		fmt.Printf("s[%v] readPersist logsLen:[%v] Term:[%v] X:[%v]\n", rf.me, len(logs), rf.CurrentTerm, rf.X)
	}
}

// 更新自己的commitIndex,用之前先检查,并且要Lock
func (rf *Raft) updateCommitIndex(commitIndex int) {
	fmt.Printf("s[%v] update commitIndex [%v]->[%v] logsLen:[%v]\n", rf.me, rf.commitIndex, commitIndex, rf.getLogsLen())
	//rf.commitIndex = max(min(commitIndex, rf.getLogsLen()), rf.getLogsLen())
	rf.commitIndex = min(commitIndex, rf.getLogsLen())
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	fmt.Printf("s[%v] snapshot Index:[%v] logsLen:[%v] X:[%v]\n", rf.me, index, rf.getLogsLen(), rf.X)
	// 丢弃Index之前的所有Log
	if rf.X == 0 {
		rf.Logs = rf.Logs[index-1:]
	} else {
		rf.Logs = rf.Logs[index-rf.X:]
	}
	rf.SnapshotData = snapshot
	rf.X = index
	rf.persist()
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
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if reply.FollowerTerm > rf.CurrentTerm {
		rf.increaseTerm(reply.FollowerTerm)
		ok = false
	}
	// 状态如果变更,请求作废
	if args.LeaderTerm != rf.CurrentTerm || rf.X != args.LastIncludeIndex {
		ok = false
	}
	if ok {
		fmt.Printf("L[%v] push Snapshot[Index:%v Term:%v] to F[%v] success\n", rf.me, args.LastIncludeIndex, args.LastIncludeTerm, server)
	}
	return ok
}

// InstallSnapshot Follower,接收Leader发来的Snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.LeaderTerm > rf.CurrentTerm {
		rf.increaseTerm(args.LeaderTerm)
	}
	reply.FollowerTerm = rf.CurrentTerm
	if rf.CurrentTerm >= args.LeaderTerm {
		rf.Logs = make([]ApplyMsg, 0)
		lastLog := ApplyMsg{
			CommandIndex: args.LastIncludeIndex,
			CommandTerm:  args.LastIncludeTerm,
		}
		rf.Logs = append(rf.Logs, lastLog)
		rf.SnapshotData = args.SnapshotData
		rf.X = args.LastIncludeIndex
		rf.commitIndex = rf.X
		rf.persist()
	}
	rf.mu.Unlock()
}

// RequestVote Follower,给Candidate投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.CandidateTerm > rf.CurrentTerm {
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
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start 接受一条来自客户端的Log
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
		fmt.Printf("L[%v] Leader get a Start request[%v], Index:[%v] Term:[%v]\n", rf.me, command, index, term)
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
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
			fmt.Printf("s[%v] start a election now,Term:[%v] log:[%v]\n", rf.me, rf.CurrentTerm, rf.getLogsLen())
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(TickerSleepTime)
	}
}

// 提交Log到状态机,更新lastAppliedIndex为commitIndex
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.commitIndex = min(rf.commitIndex, rf.getLogsLen())
		if rf.commitIndex > rf.lastAppliedIndex {
			fmt.Println(rf.me, rf.X, rf.Logs)
			// 有Log没有被提交上去,开始提交
			for index := rf.lastAppliedIndex + 1; index <= rf.commitIndex; index++ {
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
				if log != nil {
					rf.mu.Unlock()
					rf.applyCh <- *log
					rf.mu.Lock()
				}
				if rf.role == Leader {
					fmt.Printf("L[%v] apply log[%v]:%v\n", rf.me, index, rf.getLog(index))
				}
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
			fmt.Printf("C[%v] another server is leader now, Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			return
		} else if voteCount > len(rf.peers)/2 {
			// 2.赢得了大部分选票,成为Leader
			fmt.Printf("L[%v] is a Leader now, Term:[%v]\n", rf.me, rf.CurrentTerm)
			rf.role = Leader
			// 初始化Leader需要的内容
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					// 初始化nextIndex为Leader的最后条目索引+1
					rf.nextIndex[server] = rf.getLogsLen() + 1
					// 持续通过检查matchIndex给Follower发送新的Log
					go rf.pushLogsToFollower(server)
				}
			}
			rf.mu.Unlock()
			// 持续向所有Peers发送心跳包,以及检查是否要提交Logs
			go rf.sendHeartBeatsToAll()
			go rf.checkCommittedLogs()
			return
		} else if rf.isHeartBeatTimeOut() {
			// 3.选举超时,重新开始选举
			fmt.Printf("s[%v] election timeout, restart election now, Term:[%v]\n", rf.me, rf.CurrentTerm)
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
		// 检查投票结果
		rf.peersVoteGranted[server] = false
		// 如果Term对不上(比如RPC延迟了很久)不能算入票数
		if ok && reply.VoteGranted && args.CandidateTerm == rf.CurrentTerm {
			rf.peersVoteGranted[server] = true
		} else if reply.FollowerTerm > rf.CurrentTerm {
			// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
			rf.increaseTerm(reply.FollowerTerm)
		}
	}
	// args保持一致
	rf.mu.Lock()
	askVoteArgs := &RequestVoteArgs{
		CandidateTerm:  rf.CurrentTerm,
		CandidateIndex: rf.me,
	}
	// 填入Leader的最后一条Log的Term和Index,给Follower比较谁的日志更新
	if rf.getLogsLen() > 0 {
		askVoteArgs.CandidateLastLogIndex = rf.getLogsLen()
		askVoteArgs.CandidateLastLogTerm = rf.getLog(-1).CommandTerm
	}
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go askVote(server, askVoteArgs)
		}
	}
}

// Leader,检查是否可以更新commitIndex
func (rf *Raft) checkCommittedLogs() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
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
			fmt.Printf("L[%v] update commitIndex to [%v]\n", rf.me, N)
			rf.updateCommitIndex(N)
		}
		rf.mu.Unlock()
		time.Sleep(checkCommittedLogsTime)
	}
}

// Leader,持续推送Logs给Follower
func (rf *Raft) pushLogsToFollower(server int) {
	// 如果推送失败就要立即Retry,成为Leader时Push一次来加速同步
	retry := true
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != Leader {
			// 如果不是Leader了或是任期变更了,停止推送
			fmt.Printf("L[%v] stop push entry to F[%v], Role:[%v] Term:[%v]\n", rf.me, server, rf.role, rf.CurrentTerm)
			rf.mu.Unlock()
			return
		}
		if rf.getLogsLen() >= rf.nextIndex[server] || retry {
			fmt.Printf("L[%v] push to F[%v]\n", rf.me, server)
			nextIndex := rf.nextIndex[server]
			if nextIndex <= rf.X && rf.X != 0 {
				fmt.Printf("L[%v] push Snapshot to s[%v]\n", rf.me, server)
				// Leader没有用于同步的Log,推送Snapshot,第X条Log只能用于校验,其内容在Snapshot里
				ok := rf.sendInstallSnapshot(server)
				if ok {
					rf.matchIndex[server] = rf.X
					rf.nextIndex[server] = rf.X + 1
				}
				// 无论如何都要retry,即使Push成功也要追加没有在Snapshot的Log
				retry = true
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
					// 推送请求失败,可能是超时,丢包,或者Leader状态改变
					retry = true
					fmt.Printf("L[%v] push log to F[%v] timeout or status changed!\n", rf.me, server)
					rf.mu.Unlock()
					continue
				} else if pushReply.Success {
					// 检查推送完成后是否为最新,不为则继续Push,更新Follower的nextIndex,matchIndex
					retry = rf.getLogsLen() != pushLastIndex
					rf.nextIndex[server] = pushLastIndex + 1
					rf.matchIndex[server] = pushLastIndex
					fmt.Printf("L[%v] push log to F[%v] success,Leader LogsLen:[%v] pushLast:[%v] pushLen:[%v]\n", rf.me, server, rf.getLogsLen(), pushLastIndex, len(pushLogs))
				} else if pushReply.FollowerTerm > rf.CurrentTerm {
					// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
					rf.increaseTerm(pushReply.FollowerTerm)
					rf.mu.Unlock()
					return
				} else {
					// 校验失败但是请求成功,减少nextIndex且重试
					retry = true
					if pushReply.ConflictTerm != -1 {
						flag := false
						// 如果Leader有ConflictTerm的Log,将nextIndex设为对应Term中的最后一条的下一条
						for index := 0; index < rf.getLogsLen(); index++ {
							if !flag && rf.Logs[index].CommandTerm == pushReply.ConflictTerm {
								flag = true
							} else if flag && rf.Logs[index].CommandTerm != pushReply.ConflictTerm {
								// Leader有对应Term的Log
								rf.nextIndex[server] = rf.Logs[index].CommandIndex
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
					fmt.Printf("L[%v] change F[%v] nextIndex:[%v]->[%v]\n", rf.me, server, nextIndex, rf.nextIndex[server])
				}
			}
		}
		rf.mu.Unlock()
		// 如果需要重试,不等待
		if !retry {
			time.Sleep(PushLogsTime)
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
	// 检查Leader状态是否改变,改变的话此条RPC作废
	if args.LeaderTerm != rf.CurrentTerm || rf.nextIndex[server] != nextIndex || rf.role != Leader {
		ok = false
	}
	return ok, reply
}

// Leader,持续发送心跳包给所有人
func (rf *Raft) sendHeartBeatsToAll() {
	// func: 发送单个心跳包给某服务器
	sendHeartBeat := func(server int, args *AppendEnTriesArgs) bool {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
		if reply.FollowerTerm > rf.CurrentTerm {
			rf.increaseTerm(reply.FollowerTerm)
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
	// 如果接收到的RPC请求或响应中,任期号T>CurrentTerm,那么就令currentTerm等于T,并且切换状态为Follower
	if args.LeaderTerm > rf.CurrentTerm {
		rf.increaseTerm(args.LeaderTerm)
	} else if args.LeaderTerm < rf.CurrentTerm {
		// 不正常的包,Leader的Term小于Follower的Term
		reply.Success = false
		return
	}
	// 检查没有问题,不管是不是心跳包,更新选举超时时间
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	rf.role = Follower
	if args.PrevLogIndex < rf.X && rf.X != 0 {
		// 如果Leader校验的Index在Follower的X之前,直接要求Leader发送Snapshot
		reply.Success = false
		reply.ConflictIndex = 0
		reply.ConflictTerm = -1
	} else if args.PrevLogIndex == 0 {
		// 如果是第一条Log不校验
		if len(args.Logs) > 0 {
			// 防止空的包延迟到达后清空了Follower的Log
			rf.Logs = args.Logs
		}
		reply.Success = true
		rf.persist()
	} else if args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm == args.PrevLogTerm {
		// 校验正常,逐条追加
		for _, msg := range args.Logs {
			if msg.CommandIndex <= rf.getLogsLen() {
				// Follower在这个Index有Log,如果Term不相同删除从此条Log开始的所有Log
				if rf.getLog(msg.CommandIndex).CommandTerm != msg.CommandTerm {
					if rf.X == 0 {
						rf.Logs = rf.Logs[:msg.CommandIndex-1]
					} else {
						rf.Logs = rf.Logs[:msg.CommandIndex-rf.X]
					}
					rf.Logs = append(rf.Logs, msg)
				}
			} else {
				// Follower在这个Index没有Log直接追加
				rf.Logs = append(rf.Logs, msg)
			}
		}
		reply.Success = true
		rf.persist()
	} else if args.PrevLogIndex <= rf.getLogsLen() && rf.getLog(args.PrevLogIndex).CommandTerm != args.PrevLogTerm {
		// 发生冲突,Prev索引相同任期不同,返回冲突的任期和该任期的第一条Log的Index
		reply.ConflictTerm = rf.getLog(args.PrevLogIndex).CommandTerm
		for index := 1; index <= rf.getLogsLen(); index++ {
			if rf.Logs[index-1].CommandTerm == reply.ConflictTerm {
				reply.ConflictIndex = index
				break
			}
		}
		reply.Success = false
		return
	} else {
		// 校验失败.Follower在PreLogIndex的位置没有条目
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLogsLen()
		reply.Success = false
		return
	}
	// 更新最后一条新Entry的下标,如果校验失败不会走到这里
	if len(args.Logs) > 0 && args.Logs[len(args.Logs)-1].CommandIndex >= rf.lastNewEntryIndex {
		rf.lastNewEntryIndex = args.Logs[len(args.Logs)-1].CommandIndex
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex && rf.lastNewEntryIndex > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, rf.lastNewEntryIndex))
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.VotedFor = -1
	rf.peersVoteGranted = make([]bool, len(peers))
	rf.role = Follower
	rf.heartBeatTimeOut = time.Now().Add(getRandElectionTimeOut())
	rf.applyCh = applyCh
	rf.Logs = make([]ApplyMsg, 0)
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applier()
	return rf
}
