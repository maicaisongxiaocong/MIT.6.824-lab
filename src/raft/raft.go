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
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
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

var HeartBeatTimeout = 100 * time.Millisecond

// 日志项
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state

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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.status == "leader")
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
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//fmt.Printf("[ persist() ] raft(%v) {term:%v;voteFor:%v;logs:%v}  \n", rf.me, rf.term, rf.voteFor, rf.logs)

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
	if d.Decode(&logs) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil {
		log.Fatal("rf read persist err!")
	} else {
		rf.logs = logs
		rf.term = term
		rf.voteFor = voteFor
	}
	fmt.Printf("[ readPersist() ] raft(%v)  \n", rf.me)
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
		return
	}

	//情况2: 已经投票,且不是投的args.CandidateId,拒绝
	if args.Term == rf.term && rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.Sign = AlreadyVote
		return
	}

	//情况3: 日志不是最新的,拒绝
	//(论文5.4.1提到的election construction:
	//If the logs have last entries with different terms, then
	//the log with the later term is more up-to-date. If the logs
	//end with the same term, then whichever log is longer is
	//more up-to-date.

	//获得当前raft的logs的最新logEntry的index和term
	//panic: runtime error: index out of range [1] with length 1
	//注意-1
	currentLogIndex := len(rf.logs) - 1
	currentLogTerm := 0

	if currentLogIndex > 0 {
		currentLogTerm = rf.logs[currentLogIndex].Term
	}

	if currentLogTerm > args.LastLogTerm ||
		(currentLogTerm == args.LastLogTerm && currentLogIndex > args.LastLogIndex) {
		reply.Sign = OldLog
		return
	}

	//剩下的情况同意投票

	//情况4:请求raft的任期大,同意投票,如果当前raft为candidate,变为follower
	if args.Term > rf.term { //candidate 发现比自己高的任期的candidate发来投票请求
		rf.term = args.Term
		rf.voteFor = -1

		rf.persist()
		rf.voteCount = 0

		if rf.status != "candidate" {
			rf.status = "follower"
		}

	}

	//情况5:未投过票的raft,或者在情况2中的raft(由candidate变为follower的raft),

	reply.VoteGranted = true
	reply.Sign = SuccessVote

	rf.voteFor = args.CandidateId
	rf.persist()

	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)

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
		fmt.Printf("	拒绝投票!(任期旧) 	raft(%d)(term:%d) 拒绝给	raft(%d)(term:%d) 投票\n", server, reply.Term, rf.me, rf.term)
		rf.status = "follower"
		rf.term = reply.Term

		rf.voteFor = -1
		rf.persist()
		rf.voteCount = 0
		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.timer.Reset(rf.overtime)
		return false
	}

	if reply.Sign == AlreadyVote {
		fmt.Printf("	拒绝投票!(投了其他raft) 	raft(%d)(term:%d) 拒绝给	raft(%d)(term:%d) 投票\n", server, reply.Term, rf.me, rf.term)
		return false
	}

	if reply.Sign == OldLog {
		fmt.Printf("	拒绝投票!(日志旧) raft(%d)(term:%d) 拒绝给	raft(%d)(term:%d) 投票\n", server, reply.Term, rf.me, rf.term)
		return false
	}

	//情况3: 允许投票,记录票数
	fmt.Printf("	同意投票! 	raft(%d)(term:%d) 同意给	raft(%d)(term:%d) 投票\n", server, reply.Term, rf.me, rf.term)
	if rf.voteCount <= len(rf.peers)/2 {
		rf.voteCount++
	}

	if rf.voteCount > len(rf.peers)/2 {
		// 变为leader后,又接到投票,直接返回(因为超过一半投票后就变为leader,后面的投票可能已经投了但由于网络还没有收到)
		if rf.status == "leader" {
			return true
		}

		rf.status = "leader"
		fmt.Printf("新leader!!     raft(%d)已经有选票%d,已经进入leader状态\n", rf.me, rf.voteCount)
		//2b 初始化nextIndex[],默认leader的所有日志都已经匹配,所有下一个需要匹配的logEntry为len(rf.logs)
		for i, _ := range rf.nextIndex {
			if i == rf.me {
				continue
			}
			//2b: (initialized to leader last log index + 1)
			rf.nextIndex[i] = len(rf.logs)
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		reply.Sign = KilledEntry
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("收到日志追加请求!\n收到日志追加请求! raft%v(term:%v;preLogindex:%v;preLogterm:%v;committedIndex:%v;entries:%v)\n收到日志追加请求! 发给raft%v(term:%v;lastLogIndex:%v;committedIndex:%v;logs:%v)的追加请求\n",
	//	args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries, rf.me, rf.term, (len(rf.logs) - 1), rf.committedIndex, rf.logs)
	fmt.Printf("收到日志追加请求! raft%v(term:%v;preLogindex:%v;preLogterm:%v;committedIndex:%v)\n收到日志追加请求! 发给raft%v(term:%v;lastLogIndex:%v;committedIndex:%v;)的追加请求\n",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.me, rf.term, (len(rf.logs) - 1), rf.committedIndex)

	// Your code here (2A, 2B).
	reply.Term = rf.term

	//情况1: 收到的rpc的term太旧,拒绝追加
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.term {
		reply.Sign = LowTerm
		return
	}

	//情况2: index或者term不匹配

	currentIndex := len(rf.logs) - 1
	//currentTerm := 0
	if currentIndex > 0 {
		//panic: runtime error: index out of range [1] with length 1
		// currentTerm := len(rf.logs) - 1,必须减1
		//currentTerm = rf.logs[currentIndex].Term
	}

	//leader 给disconnected()的raft 发appendEntries() rpc
	//当raft 又 connected()后 之前的appendEntries() rpc的到响应,这里把他门排除
	if args.LeaderCommit < rf.committedIndex {
		reply.Sign = LowCommittedIndex
		return
	}
	//2, Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > currentIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Sign = MismatchIndex
		return
	}

	//以下情况返回成功
	reply.Sign = SuccessEntry

	//情况3: 日志不为空,删除该raft的logs匹配日志项后面的所有日志项,然后追加日志
	if len(args.Entries) != 0 {
		//3, If an existing entry conflicts with a new one (same index but different terms),
		//delete the existing entry and all that follow it (§5.3)
		rf.logs = rf.logs[:args.PrevLogIndex+1]

		//4, Append any new entries not already in the log
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	//情况4: 心跳功能
	if rf.term < args.Term {
		rf.status = "follower"
		rf.term = args.Term
		rf.voteCount = 0
		rf.voteFor = -1
		rf.persist()
	}

	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)

	//情况5: commit日志功能
	//5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.committedIndex {
		fmt.Printf("同步committedndex!\n同步committedndex! raft%d(committedIndex:%v)\n同步committedndex! 给raft%d(committedIndex:%v)(lastLagIndex: %v)\n", args.LeaderId, args.LeaderCommit, rf.me, rf.committedIndex, len(rf.logs)-1)
		rf.committedIndex = func(a int, b int) int {
			if a < b {
				return a
			}
			return b
		}(args.LeaderCommit, len(rf.logs)-1) // len(rf.logs) - 1

	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, LogsTemp []LogEntry) bool {
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok

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
		fmt.Printf("追加日志失败!(任期旧)		变为follower,%d号任务给%d号任务追加日志\n", rf.me, server)
		rf.status = "follower"
		rf.term = reply.Term

		rf.voteFor = -1
		rf.persist()

		rf.voteCount = 0
		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.timer.Reset(rf.overtime)
		return false

	}

	//情况3: 若是因为PrevLogIndex或PrevLogTerm不匹配,nextindex减1,重传rpc
	if reply.Sign == MismatchIndex {
		fmt.Printf("追加日志失败!(index不匹配)		%d号任务给%d号任务追加日志\n", rf.me, server)

		rf.nextIndex[server]--

		fmt.Printf("raft[%v]		{nextIndex:%v}\n", rf.me, rf.nextIndex)

		reply := AppendEntriesReply{}

		arg := AppendEntriesArgs{
			Term:     args.Term,
			LeaderId: args.LeaderId,

			PrevLogIndex: args.PrevLogIndex - 1, //减少1
			PrevLogTerm:  0,
			Entries:      nil,

			LeaderCommit: args.LeaderCommit,
		}

		if arg.PrevLogIndex > 0 {
			arg.PrevLogTerm = rf.logs[arg.PrevLogIndex].Term
		}
		//重新复制entries
		if rf.nextIndex[server] <= len(LogsTemp)-1 && rf.nextIndex[server] > 0 {
			arg.Entries = append(arg.Entries, LogsTemp[rf.nextIndex[server]:]...)
		}

		go rf.sendAppendEntries(server, &arg, &reply, LogsTemp)
		return false
	}

	//接下都是追加成功的

	//情况4: 没有日志,心跳功能成功
	if len(args.Entries) == 0 {
		fmt.Printf("追加日志成功!(心跳)		%d号任务给%d号任务追加日志\n", rf.me, server)
		return true
	}

	//情况5: 有日志,追加成功,则该改变对应的nextIndex,统计追加成功的raft个数
	rf.nextIndex[server] += len(args.Entries)
	fmt.Printf("追加日志成功!		%d号任务给%d号任务追加日志\n", rf.me, server)

	if rf.appendCount <= len(rf.peers)/2 {
		rf.appendCount++
	}

	if rf.appendCount > len(rf.peers)/2 {
		//日志已经已经committed了,不用再提交,保证幂等性
		if args.LeaderCommit < rf.committedIndex {
			return true
		}
		//rf.committedIndex = rf.committedIndex + len(args.Entries)
		if rf.committedIndex < args.PrevLogIndex+len(args.Entries) {

			rf.committedIndex = args.PrevLogIndex + len(args.Entries)
			fmt.Printf("成功被大多数raft接受!		leader(%d) committedIndex:%v\n", rf.me, rf.committedIndex)

		}
	}

	return true
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	//1: if this server isn't the leader, returns false.

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" {
		return index, term, isLeader
	}

	//2: 附加
	isLeader = true

	entry := LogEntry{Command: command, Term: rf.term}
	rf.logs = append(rf.logs, entry)
	rf.persist()

	index = len(rf.logs) - 1
	term = rf.term
	//fmt.Printf("客户提交请求! end lastLagIndex = %v;\n客户提交请求! logs:%v\n", len(rf.logs)-1, rf.logs)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(1 * time.Millisecond) //让测试标题输出比这里面的输出早

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timer.C:
			//2 follower先将自己的状态变为candidate,然后为为自己投票,总票数加1,
			rf.mu.Lock()
			//defer rf.mu.Unlock() 这样写,一直无法释放锁,应该写到select 语句最后

			switch rf.status {

			case "follower":
				rf.status = "candidate"
				fallthrough
			case "candidate":
				rf.term++
				rf.persist()

				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				rf.voteFor = rf.me
				fmt.Printf("(轮数:%d)candidate!!!		第%d号任务已经进入candidate状态\n", rf.term, rf.me)
				rf.persist()

				rf.voteCount = 1 //rf.voteCount++错误,率先进入第三轮选举就会被当选

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					arg := RequestVoteArgs{
						Term:         rf.term,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if arg.LastLogIndex > 0 {
						arg.LastLogTerm = rf.logs[arg.LastLogIndex].Term
					}

					reply := RequestVoteReply{}
					fmt.Printf("发起投票! 	第%d号任务(term:%d)向第%d号任务发起投票\n", rf.me, rf.term, i)

					go rf.sendRequestVote(i, &arg, &reply)
				}
			case "leader":
				rf.timer.Reset(HeartBeatTimeout)
				LogsTemp := rf.logs //保证下面AppendEntries线程操作的是同一个logs
				rf.appendCount = 1

				for j, _ := range rf.peers {
					if j == rf.me {
						continue
					}
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
						fmt.Printf("raft(%v)	term:%v;PreLogIndex:%v;logs:%v;to raft(%v)\n", rf.me, rf.term, arg.PrevLogIndex, len(rf.logs)-1, j)
						arg.PrevLogTerm = LogsTemp[arg.PrevLogIndex].Term
					}

					//paper:If last log index ≥ nextIndex for a follower:
					if rf.nextIndex[j] <= len(LogsTemp)-1 {
						// 保证循环各个raft追加过程中,logs[]一样,引入LogsTemp
						arg.Entries = append(arg.Entries, LogsTemp[rf.nextIndex[j]:]...)
					}

					go rf.sendAppendEntries(j, &arg, &reply, LogsTemp)
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

	rf.status = "follower"
	rf.term = 0

	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//同步appliedndex
	go rf.synchronizeAppliedIndex()

	return rf
}

func (rf *Raft) synchronizeAppliedIndex() {

	for true {

		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		//只有leader能够同步自己的appliedIndex,其他只能通过appendntries() 被迫同步
		//if rf.status != "leader" || rf.appliedIndex == rf.committedIndex {
		if rf.appliedIndex == rf.committedIndex {
			rf.mu.Unlock()
			continue
		}

		fmt.Printf("提交状态机!raft(%v)	start appliedIndex:%v  committedIndex:%v\n", rf.me, rf.appliedIndex, rf.committedIndex)
		for rf.appliedIndex < rf.committedIndex {

			rf.appliedIndex++

			am := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.appliedIndex].Command,
				CommandIndex: rf.appliedIndex,
			}
			rf.appliedChan <- am
		}
		fmt.Printf("提交状态机!raft(%v)	end appliedIndex:%v  committedIndex:%v \n", rf.me, rf.appliedIndex, rf.committedIndex)
		rf.mu.Unlock()
	}
}
