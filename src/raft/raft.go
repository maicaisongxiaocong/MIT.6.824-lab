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
//   ask a Raft for its current term, and whether it thinks it is LEADER
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"log"
	"math/rand"

	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

var HeartBeatTimeout = 50 * time.Millisecond

// 日志项
type LogEntry struct {
	Term    int
	Command interface{}
}

// raft状态枚举
const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	//mu sync.Mutex // Lock to protect shared access to this peer's state
	mu        deadlock.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	status            string    //状态
	term              int       //第几轮投票
	electionStartTime time.Time //新一轮投票开始时间
	voteFor           int       //为谁投票,-1表示还没投票
	voteCount         int       //获得总票数,初始为0
	overtime          time.Duration
	timer             *time.Ticker

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs        []LogEntry
	appliedChan chan ApplyMsg

	committedIndex int
	appliedIndex   int

	appendCount int //统计成功追加日志的raft有多少个

	//todo: 作用区别
	nextIndex  []int
	matchIndex []int

	//2d
	lastIncludeIndex int //最近一次snapshot 的index 用来计算log的没有压缩日志状态的下标(index + lastIncludeIndex)
	lastIncludeTerm  int
}

// return currentTerm and whether this server
// believes it is the LEADER.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.status == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.logs)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.committedIndex)
	//2d
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//fmt.Printf("[persist] (raft%v) {term%v}\n", rf.me, rf.term)

}

// restore previously persisted state.
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var term int
	var voteFor int
	var committedIndex int
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&logs) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&committedIndex) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		log.Fatal("rf read persist err!")
	} else {
		rf.logs = logs
		rf.term = term
		rf.voteFor = voteFor
		rf.committedIndex = committedIndex
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.appliedIndex = lastIncludeIndex
	}
	fmt.Printf(" [readPersist] (raft%v)\n", rf.me)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//必须放在剪切日志之前,否则log中不存在了
	rf.lastIncludeTerm = rf.logs[rf.getAfterSnapshotLogIndex(index)].Term

	//1 剪日志
	tlog := make([]LogEntry, 1) //第一个日志元素当作哨兵
	tlog = append(tlog, rf.logs[rf.getAfterSnapshotLogIndex(index+1):]...)
	rf.logs = tlog

	//一定要放在剪切日志之后,否则rf.getAfterSnapshotLogIndex(index+1) 保持不变
	rf.lastIncludeIndex = index

	rf.persist()

	fmt.Printf("[snapshot()] (raft%v) {lastIncludeIndex%v,lastLogIndex%v,lastIncludeTerm%v}\n", rf.me, rf.lastIncludeIndex, len(rf.logs)-1, rf.lastIncludeTerm)

	//2 持久化snapshot
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, snapshot)

}

type InstallSnapshotArgs struct {
	Term              int    //LEADER’s term
	LeaderId          int    //so FOLLOWER can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //
	Data              []byte //序列化snapshot
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term

	//1 对于term过期的rpc,直接返回
	if args.LastIncludedIndex < rf.lastIncludeIndex || args.LastIncludedIndex < rf.committedIndex {
		return
	}
	fmt.Printf("[InstallSnapshot()] (raft%v,term%v to raft%v,term%v)\n", args.LeaderId, args.Term, rf.me, rf.term)

	//2 请求rpc的raft term小
	if args.Term < rf.term {
		return
	}

	//3 lastIncludIndex < rf.lastIncludIndex
	if args.LastIncludedIndex < rf.lastIncludeIndex { //todo 还用考虑Term 不相等么
		return
	}

	//4 成功执行,更新日志,更新snapshot

	//转变follower
	if args.Term > rf.term && rf.status != FOLLOWER {
		rf.status = FOLLOWER
		rf.voteFor = -1
		rf.voteCount = 0
		rf.term = args.Term
		rf.persist()
	}

	//重置投票倒计时
	rf.resetVoteOverTime()

	//剪切日志
	//情况1 日志过长,留下snapshot当前index 后的所有日志
	xLog := make([]LogEntry, 1) //第一个日志项为哨兵,term = 0 ,command = nil
	if args.LastIncludedIndex < rf.getPostSnapshotLogIndex(len(rf.logs)-1) {

		xLog = append(xLog, rf.logs[rf.getAfterSnapshotLogIndex(rf.lastIncludeIndex+1):]...)

	}
	//情况2 日志短 抛弃原来所有的日志
	rf.logs = xLog
	rf.persist()

	//更新snapshot
	snapshot := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	rf.appliedChan <- snapshot

	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, args.Data)

	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.appliedIndex = rf.lastIncludeIndex
	rf.committedIndex = rf.lastIncludeIndex
	rf.persist()

}

func (rf *Raft) sentInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for ok == false {

		if rf.killed() {
			return false
		}

		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 过期的rpc请求不做后处理
	if args.Term < rf.term {
		return false
	}

	//1 更新nextIndex
	rf.nextIndex[server] = rf.lastIncludeIndex + 1

	//2 当前raft的term小于请求rpc的raft的term
	//转化为follower
	if reply.Term > rf.term {
		rf.status = FOLLOWER
		rf.voteFor = -1
		rf.voteCount = 0
		rf.term = reply.Term

		rf.resetVoteOverTime()
		fmt.Printf("[sendInstallSnapshot] [shift to follower] {raft%v,term%v to raft%v,reply.Term%v }\n", args.LeaderId, args.Term, server, reply.Term)
		return true
	}

	fmt.Printf("[sentInstallSnapshot] [Success] {raft%v,term%v to raft%v lastIncludeIndex %v,lastIncludeTerm %v}\n", args.LeaderId, args.Term, server, args.LastIncludedIndex, args.LastIncludedTerm)

	return true
}

// 当前日志下标为i 求如果没有snapshot 原本的下标该是多少
func (rf *Raft) getPostSnapshotLogIndex(afterSnapshotIndex int) int {
	return afterSnapshotIndex + rf.lastIncludeIndex
}

// 根据不剪切的日志下标求剪切后的日志下标
func (rf *Raft) getAfterSnapshotLogIndex(postSnapshotIndex int) int {
	return postSnapshotIndex - rf.lastIncludeIndex
}

// 重置投票倒计时
func (rf *Raft) resetVoteOverTime() {
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)
}

const (
	KilledVote = iota
	OldTerm
	OldLog
	AlreadyVote
	SuccessVote
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	//2b
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool

	Sign int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if rf.killed() {
		reply.Sign = KilledVote
		return
	}

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.term

	//情况1: 请求raft的任期小,拒绝
	if args.Term < rf.term {
		reply.Sign = OldTerm
		fmt.Printf("RequestVote() [OldTerm] {raft%v term%v	to raft%v term%v}\n", args.CandidateId, args.Term, rf.me, rf.term)
		return
	}

	//情况2: 已经投票,且不是投的args.CandidateId
	if args.Term == rf.term && rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.Sign = AlreadyVote
		fmt.Printf("RequestVote() [AlreadyVote] {raft%v term%v	to raft%v term%v}\n", args.CandidateId, args.Term, rf.me, rf.term)
		return
	}

	//情况3: 日志不是最新的,拒绝
	//(论文5.4.1提到的election construction:
	//If the logs have last entries with different terms, then
	//the log with the later term is more up-to-date. If the logs
	//end with the same term, then whichever log is longer is
	//more up-to-date.

	currentLogIndex := rf.getPostSnapshotLogIndex(len(rf.logs) - 1)
	currentLogTerm := rf.logs[len(rf.logs)-1].Term

	//表示log长度为1(只存在下标为0的一个哨兵entry),那么term为当前shapshot的term
	if currentLogIndex == rf.lastIncludeIndex {
		currentLogTerm = rf.lastIncludeTerm
	}

	if currentLogTerm > args.LastLogTerm ||
		(currentLogTerm == args.LastLogTerm && currentLogIndex > args.LastLogIndex) {
		reply.Sign = OldLog
		fmt.Printf("[RequestVote] [OldLog] (raft%v term%v to raft%v term%v) \n{currentLogTerm%v,args.LastLogTerm%v,currentLogIndex%v,args.LastLogIndex%v}\n", args.CandidateId, args.Term, rf.me, rf.term, currentLogTerm, args.LastLogTerm, currentLogIndex, args.LastLogIndex)
		return
	}

	//剩下的情况同意投票

	//情况4:请求raft的任期大,同意投票,如果当前raft为candidate,变为follower
	if args.Term > rf.term { //CANDIDATE 发现比自己高的任期的candidate发来投票请求
		rf.term = args.Term
		rf.voteFor = -1

		rf.persist()
		rf.voteCount = 0

		if rf.status != FOLLOWER {
			rf.status = FOLLOWER
		}

	}

	//情况5:已经投过该candidateId的raft,未投过票的raft,或者在情况2中的raft(由candidate或leader变为follower的raft),

	reply.VoteGranted = true
	reply.Sign = SuccessVote
	fmt.Printf("RequestVote() [SuccessVote] {raft%v term%v	to raft%v term%v}\n", args.CandidateId, args.Term, rf.me, rf.term)

	rf.voteFor = args.CandidateId
	rf.persist()

	//重置投票倒计时
	rf.resetVoteOverTime()

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for ok == false {

		if rf.killed() {
			return false
		}

		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Sign == KilledVote {
		return false
	}

	//情况1: 收到过期的RPC,不处理
	if args.Term < rf.term {
		return false
	}

	//情况2: 不允许投票,

	if reply.Sign == OldTerm {
		fmt.Printf("[vote failed oldTerm,to FOLLOWER] (raft%d term%d rejected by raft%d term%d)\n", rf.me, rf.term, server, reply.Term)
		rf.status = FOLLOWER
		rf.term = reply.Term

		rf.voteFor = -1
		rf.persist()
		rf.voteCount = 0
		//重置投票倒计时
		rf.resetVoteOverTime()
		return false
	}

	if reply.Sign == AlreadyVote {
		fmt.Printf("[vote failed AlreadyVote] (raft%d term%d rejected by raft%d term%d)\n", rf.me, rf.term, server, reply.Term)
		return false
	}

	if reply.Sign == OldLog {
		//fmt.Printf("	拒绝投票!(日志旧) raft(%d)(term:%d) 拒绝给	raft(%d)(term:%d) 投票\n", server, reply.Term, rf.me, rf.term)
		return false
	}

	//情况3: 允许投票,记录票数
	fmt.Printf("[vote success] (raft%d term%d agreed by raft%d term%d)\n", rf.me, rf.term, server, reply.Term)
	if rf.voteCount <= len(rf.peers)/2 {
		rf.voteCount++
	}

	if rf.voteCount > len(rf.peers)/2 {
		// 变为leader后,又接到投票,直接返回(因为超过一半投票后就变为leader,后面的投票可能已经投了但由于网络还没有收到)
		if rf.status == LEADER {
			return true
		}

		rf.status = LEADER
		fmt.Printf("[新leader] (raft%d term%v) {voteCount%v}\n", rf.me, rf.term, rf.voteCount)
		//2b 初始化nextIndex[],默认leader的所有日志都已经匹配,所有下一个需要匹配的logEntry为len(rf.logs)
		for i, _ := range rf.nextIndex {
			if i == rf.me {
				continue
			}
			//2b: (initialized to LEADER last log index + 1)
			rf.nextIndex[i] = rf.getPostSnapshotLogIndex(len(rf.logs))
		}
		rf.timer.Reset(HeartBeatTimeout)

	}

	return true
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//2b 参照figure2中AppendEntries RPC

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

// AppendEntriesReply 中Sign的枚举
const (
	KilledEntry       int = iota
	LowTerm               //任期过时
	MismatchIndex         //term或者preLogIndex不匹配
	LowCommittedIndex     //同一任期的leader，之前发的追加日志rpc 现在才被raft接收//committedIndex比raft小
	SuccessEntry          //成功
)

type AppendEntriesReply struct {
	Term int
	Sign int

	//优化日志匹配
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		reply.Sign = KilledEntry
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("AppendEntries()   {	raft:%v	term:%v	preLogindex:%v	log.len:%v	committedIndex:%v	}\n			   {	raft%v	term:%v	lastLogIndex:%v	committedIndex:%v	}\n",
	//	args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries), args.LeaderCommit, rf.me, rf.term, (len(rf.logs) - 1), rf.committedIndex)

	// Your code here (2A, 2B).
	reply.Term = rf.term

	//情况1: 收到的rpc的term太旧,拒绝追加
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.term {
		reply.Sign = LowTerm
		return
	}

	//同一term但是committedIndex小
	//LEADER 给disconnected()的raft 发appendEntries() rpc
	//当raft 又 connected()后 之前的appendEntries() rpc得到响应,这里把他门排除
	if args.LeaderCommit < rf.committedIndex { //todo 单独考虑
		reply.Sign = LowCommittedIndex
		return
	}

	//重置投票倒计时
	rf.resetVoteOverTime()

	//任期小 变为follower 并改变term
	if rf.term < args.Term {
		rf.status = FOLLOWER
		rf.term = args.Term
		rf.voteCount = 0
		rf.voteFor = -1
		rf.persist()
	}

	//情况3: 下标不匹配
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

	postCurrentLastIndex := rf.getPostSnapshotLogIndex(len(rf.logs) - 1)

	postPrevLogTerm := 0
	if rf.getAfterSnapshotLogIndex(args.PrevLogIndex) <= len(rf.logs)-1 &&
		rf.getAfterSnapshotLogIndex(args.PrevLogIndex) > 0 {
		postPrevLogTerm = rf.logs[rf.getAfterSnapshotLogIndex(args.PrevLogIndex)].Term
	}

	if args.PrevLogIndex == rf.lastIncludeIndex {
		postPrevLogTerm = rf.lastIncludeTerm
	}

	if args.PrevLogIndex > postCurrentLastIndex || args.PrevLogTerm != postPrevLogTerm {
		reply.Sign = MismatchIndex

		if args.PrevLogIndex > postCurrentLastIndex {
			reply.ConflictIndex = postCurrentLastIndex
			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = postPrevLogTerm
			for i := 1; i <= rf.getAfterSnapshotLogIndex(args.PrevLogIndex); i++ {
				if rf.logs[i].Term == rf.logs[rf.getAfterSnapshotLogIndex(args.PrevLogIndex)].Term {
					reply.ConflictIndex = rf.getPostSnapshotLogIndex(i)
					break
				}
			}
		}
		return
	}

	//以下情况返回成功
	reply.Sign = SuccessEntry

	//情况4:下标匹配 日志不为空,删除该raft的logs匹配日志项后面的所有日志项,然后追加日志
	if len(args.Entries) != 0 {
		//3, If an existing entry conflicts with a new one (same index but different terms),
		//delete the existing entry and all that follow it (§5.3)
		rf.logs = rf.logs[:rf.getAfterSnapshotLogIndex(args.PrevLogIndex+1)]

		//4, Append any new entries not already in the log
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	// commit日志功能
	//5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.committedIndex {
		fmt.Printf("[appendEntry commit]	(raft%d committed%v to raft%d committed%v lastLogIndex%v)\n", args.LeaderId, args.LeaderCommit, rf.me, rf.committedIndex, rf.getPostSnapshotLogIndex(len(rf.logs)-1))
		rf.committedIndex = func(a int, b int) int {
			if a < b {
				return a
			}
			return b
		}(args.LeaderCommit, rf.getPostSnapshotLogIndex(len(rf.logs)-1))
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, LogsTemp []LogEntry) bool {

	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for ok == false {

		if rf.killed() {
			return false
		}

		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Sign == KilledEntry {
		return false
	}

	//情况1: 收到 之前term的过期的rpc回复
	if args.Term < rf.term {
		//fmt.Printf("过期日志rpc回复(term小)! raft(%v,oldterm:%v;nowterm%v) to raft(%v)\n", rf.me, args.Term, rf.term, server)
		return false
	}

	//情况1.2: 收到同一term 但是committedIndex小的过期rpc回复
	if reply.Sign == LowCommittedIndex {
		//fmt.Printf("过期日志rpc回复(committedIndex小)! raft(%v,oldcommittedIndex:%v;nowcommittedIndex%v) to raft(%v)\n", rf.me, args.LeaderCommit, rf.committedIndex, server)
		return false
	}

	//保证幂等性  之前的rpc回复 最近才处理, 详情见错误3.1
	if args.PrevLogIndex != rf.nextIndex[server]-1 {
		return false
	}

	//情况2: 若是因为任期太旧,leader转为follower
	if reply.Sign == LowTerm {
		fmt.Printf("[LowTerm] [to follower] (raft%d term%v to raft%d term%v)\n", rf.me, args.Term, server, reply.Term)
		rf.status = FOLLOWER
		rf.term = reply.Term

		rf.voteFor = -1
		rf.persist()

		rf.voteCount = 0
		//重置投票倒计时
		rf.resetVoteOverTime()
		return false

	}

	//情况3: 若是因为PrevLogIndex或PrevLogTerm不匹配,nextindex减1,重传rpc
	if reply.Sign == MismatchIndex {
		fmt.Printf("[mismatchIndex] (raft%v to raft%v ) {nextIndex%v conflictIndex%v conflictTerm%v}\n", rf.me, server, rf.nextIndex[server], reply.ConflictIndex, reply.ConflictTerm)
		//prelogIndex > currentLogIndex 和 当前raft log中找不到comflictIndex两种情况
		rf.nextIndex[server] = reply.ConflictIndex

		i := rf.getAfterSnapshotLogIndex(args.PrevLogIndex)
		for ; i > 0; i-- {
			if reply.ConflictTerm == rf.logs[i].Term {
				rf.nextIndex[server] = rf.getPostSnapshotLogIndex(i)
				break
			}
		}

		//特殊情况,防止preLogIndex = -1,导致appendEntries()中rf.logs[args.PrevLogIndex]出现下标溢出
		//见错误3c,测试7 TestReliableChurn2C()
		if rf.nextIndex[server] == 0 {
			rf.nextIndex[server]++
		}

		return false
	}

	//接下都是追加成功的

	//情况4: 没有日志,心跳功能成功
	if len(args.Entries) == 0 {
		fmt.Printf("[heartBeatSuccess] (raft%d to raft%d)\n", rf.me, server)
		return true
	}

	//情况5: 有日志,追加成功,则该改变对应的nextIndex,统计追加成功的raft个数
	rf.nextIndex[server] += len(args.Entries)
	fmt.Printf("[SuccessEntry] (raft%d to raft%d)\n", rf.me, server)

	if rf.appendCount <= len(rf.peers)/2 {
		rf.appendCount++
	}

	if rf.appendCount > len(rf.peers)/2 {
		//日志已经已经committed了,不用再提交,保证幂等性
		if args.LeaderCommit < rf.committedIndex {
			return true
		}

		if rf.committedIndex < args.PrevLogIndex+len(args.Entries) {

			rf.committedIndex = args.PrevLogIndex + len(args.Entries)
			fmt.Printf("[commit]	(raft%d) {committedIndex%v}\n", rf.me, rf.committedIndex)

		}
	}

	return true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the LEADER, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the LEADER
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the LEADER.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	//1: if this server isn't the LEADER, returns false.

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return index, term, isLeader
	}

	//2: 附加
	isLeader = true

	entry := LogEntry{Command: command, Term: rf.term}
	rf.logs = append(rf.logs, entry)
	fmt.Printf("[start] (raft%v term%v ) {lastIncludeIndex%v, LastLogIndex%v, cmd%v}\n", rf.me, rf.term, rf.lastIncludeIndex, len(rf.logs)-1, command)
	rf.persist()

	index = rf.getPostSnapshotLogIndex(len(rf.logs) - 1)
	term = rf.term
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
	fmt.Printf("[kill] (raft%v)\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(10 * time.Millisecond) //让测试标题输出比这里面的输出早

	for rf.killed() == false {

		// Your code here to check if a LEADER election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			//defer rf.mu.Unlock() 这样写,一直无法释放锁,应该写到select 语句最后

			switch rf.status {

			case FOLLOWER:
				rf.status = CANDIDATE
				fallthrough
			case CANDIDATE:
				rf.term++
				rf.persist()

				//重置投票倒计时
				rf.resetVoteOverTime()
				rf.voteFor = rf.me
				fmt.Printf("[CANDIDATE] (raft%v term%v) {lastIncludeIndex%v,LastlogIndex%v,lastTerm%v,committedIndex%v}\n", rf.me, rf.term, rf.lastIncludeIndex, len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, rf.committedIndex)
				rf.persist()

				rf.voteCount = 1 //rf.voteCount++错误,率先进入第三轮选举就会被当选

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					arg := RequestVoteArgs{
						Term:         rf.term,
						CandidateId:  rf.me,
						LastLogIndex: rf.getPostSnapshotLogIndex(len(rf.logs) - 1),
						LastLogTerm:  0,
					}

					arg.LastLogTerm = rf.logs[len(rf.logs)-1].Term

					if arg.LastLogIndex == rf.lastIncludeIndex {
						arg.LastLogTerm = rf.lastIncludeTerm
					}

					reply := RequestVoteReply{}
					fmt.Printf("[sendRequestVote] (raft%v term%d to raft%d) \n", rf.me, rf.term, i)

					go rf.sendRequestVote(i, &arg, &reply)
				}
			case LEADER:
				rf.timer.Reset(HeartBeatTimeout)
				LogsTemp := rf.logs //保证下面AppendEntries线程操作的是同一个logs
				rf.appendCount = 1

				for j, _ := range rf.peers {
					if j == rf.me {
						continue
					}
					//情况1 发送快照
					if rf.lastIncludeIndex >= rf.nextIndex[j] { //2d 难点
						//参数准备
						args := InstallSnapshotArgs{
							Term:              rf.term,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludeIndex,
							LastIncludedTerm:  rf.lastIncludeTerm,
							Data:              rf.persister.snapshot,
						}
						reply := InstallSnapshotReply{}

						fmt.Printf("[sentInstallSnapshot] (raft%v term%v to raft%v) {LastIncludedIndex%v, LastIncludedTerm%v}\n", rf.me, rf.term, j, rf.lastIncludeIndex, rf.lastIncludeTerm)
						go rf.sentInstallSnapshot(j, &args, &reply)

					} else {

						//情况2 追加日志
						//准备参数
						reply := AppendEntriesReply{}

						arg := AppendEntriesArgs{
							Term:     rf.term,
							LeaderId: rf.me,

							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,

							LeaderCommit: rf.committedIndex,
						}

						arg.PrevLogIndex = rf.nextIndex[j] - 1

						if arg.PrevLogIndex > 0 {
							//2d 若是日志已经被剪切(prevLogIndex恰好是snapshot最后一个index),则用lastIncludeTerm赋值
							if arg.PrevLogIndex == rf.lastIncludeIndex {
								arg.PrevLogTerm = rf.lastIncludeTerm
							} else {
								arg.PrevLogTerm = LogsTemp[rf.getAfterSnapshotLogIndex(arg.PrevLogIndex)].Term
							}
						}

						//paper:If last log index ≥ nextIndex for a FOLLOWER:
						if rf.nextIndex[j] <= rf.getPostSnapshotLogIndex(len(LogsTemp)-1) {
							// 保证循环各个raft追加过程中,logs[]一样,引入LogsTemp
							arg.Entries = append(arg.Entries, LogsTemp[rf.getAfterSnapshotLogIndex(rf.nextIndex[j]):]...)
						}
						fmt.Printf("[sendAppendEntries] (raft%v term%d to raft%d) {PrevLogIndex%v, PrevLogTerm%v, logEntries.len%v}\n", rf.me, rf.term, j, arg.PrevLogIndex, arg.PrevLogTerm, len(arg.Entries))

						go rf.sendAppendEntries(j, &arg, &reply, LogsTemp)
					}

				}

			}
			rf.mu.Unlock()
		}

	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C)

	rf.status = FOLLOWER
	rf.term = 0

	rf.overtime = time.Duration(100+rand.Intn(250)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)

	rf.voteFor = -1
	rf.voteCount = 0

	//2b
	rf.appendCount = 1 //统计追加日志成功的raft的个数

	rf.logs = make([]LogEntry, 1) //paper: first index is 1
	rf.appliedChan = applyCh

	rf.appliedIndex = 0
	rf.committedIndex = 0

	//todo: 还不知道有什么用
	rf.matchIndex = make([]int, len(rf.peers))

	rf.nextIndex = make([]int, len(rf.peers))

	//2d
	rf.lastIncludeTerm = 0
	rf.lastIncludeIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//同步appliedndex

	go rf.synchronizeAppliedIndex()

	//go StartHTTPDebuger()

	return rf
}

func (rf *Raft) synchronizeAppliedIndex() {
	if rf.killed() {
		return
	}

	for true {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()

		if rf.appliedIndex == rf.committedIndex {
			rf.mu.Unlock()
			continue
		}

		message := make([]ApplyMsg, 0)
		//fmt.Printf("[syn] (raft%v) {appliedIndex%v, committedIndex%v,lastIncludeIndex%v}\n", rf.me, rf.appliedIndex, rf.committedIndex, rf.lastIncludeIndex)
		for rf.appliedIndex < rf.committedIndex {
			rf.appliedIndex++

			am := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getAfterSnapshotLogIndex(rf.appliedIndex)].Command,
				CommandIndex: rf.appliedIndex,
			}
			message = append(message, am)
		}

		//config.go 中 applierSnap()将rf.appliedChan中元素输出,
		//且输出的操作中调用了raft.go中的snapshot(),它需要占用raft的mu.Lock(),
		//这里如果连续rf.appliedChan <- am的话,因为rf.appliedChan没有缓存会阻塞,并且占有raft的mu.Lock(),
		//这里会等待applierSnap()将元素输出,
		//但是raft.go中的snapshot()会一直处于请求锁的状态,(因为这里占有锁)无法将下一元素输出
		//从而造成死锁

		//一定要释放锁
		rf.mu.Unlock()
		for _, temp := range message {
			rf.appliedChan <- temp
		}

		fmt.Printf("[synAppliedIndex!] {raft%v,appliedIndex%v,committedIndex%v}\n", rf.me, rf.appliedIndex, rf.committedIndex)
	}
}
