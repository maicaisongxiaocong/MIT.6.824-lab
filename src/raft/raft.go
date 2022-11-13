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

	overtime time.Duration
	timer    *time.Ticker

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs        []LogEntry
	appliedChan chan ApplyMsg

	committedIndex int
	appliedIndex   int

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
	isleader = rf.status == "leader"
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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("收到投票请求!	%d号任务(term:%d)收到来自%d号任务(term:%d)\n", rf.me, rf.term, args.CandidateId, args.Term)

	reply.VoteGranted = false
	reply.Term = rf.term

	//情况1: 请求raft的任期小,拒绝投票
	if args.Term < rf.term {
		return
	}

	//情况2:请求raft的任期大,同意投票,如果当前raft为candidate,变为follower
	if args.Term > rf.term { //candidate 发现比自己高的任期的candidate发来投票请求
		rf.term = args.Term

		rf.voteFor = -1
		rf.voteCount = 0

		if rf.status == "candidate" {
			rf.status = "follower"
		}

	}
	//获得当前raft的logs的最新logEntry的index和term
	currentLogIndex := len(rf.logs)
	currentLogTerm := 0
	if currentLogIndex > 0 {
		currentLogTerm = rf.logs[currentLogIndex].Term
	}

	//情况3:未投过票的raft,或者在情况2中的raft(由candidate变为follower的raft),
	//满足figure2中RequestVote第二个条件情况(论文5.4.1提到的election construction:保证选出的leader必须含有所有committed的entries),则投票
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		currentLogIndex <= args.LastLogIndex && currentLogTerm <= args.LastLogTerm {

		reply.VoteGranted = true
		rf.term = args.Term

		rf.voteFor = args.CandidateId

		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.timer.Reset(rf.overtime)
	}
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

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for ok == false {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//情况1: 收到过期的RPC,不处理
	if args.Term < rf.term {
		return false
	}

	//情况2: 允许投票
	if reply.VoteGranted == true {
		fmt.Printf("	同意投票! 	%d号任务(term:%d) 同意给	%d号任务(term:%d) 投票\n", server, rf.term, rf.me, reply.Term)
		if rf.voteCount < len(rf.peers)/2 {
			rf.voteCount++
		}

		if rf.voteCount >= len(rf.peers)/2 {
			//2b 变为leader后,又接到投票,直接返回(因为超过一半投票后就变为leader,后面的投票可能已经投了但由于网络还没有收到)
			if rf.status == "leader" {
				return true
			}

			fmt.Printf("新leader!!     第%d号任务已经有选票%d,已经进入leader状态\n", rf.me, rf.voteCount)
			rf.status = "leader"
			rf.timer.Reset(HeartBeatTimeout)
			//2b 初始化nextIndex[],默认leader的所有日志都已经匹配,所有下一个需要匹配的logEntry为len(rf.logs)
			for i, _ := range rf.nextIndex {
				//2b: (initialized to leader last log index + 1)
				rf.nextIndex[i] = len(rf.logs)
			}
		}

	} else {
		//情况3: 不允许投票,
		fmt.Printf("	拒绝投票! 	%d号任务(term:%d) 拒绝给	%d号任务(term:%d) 投票\n", server, rf.term, rf.me, reply.Term)
		if reply.Term > rf.term {
			rf.status = "follower"
			rf.term = reply.Term

			rf.voteFor = -1
			rf.voteCount = 0
			rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
			rf.timer.Reset(rf.overtime)
		}
	}

	return true
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//2b 参照figure2中AppendEntries RPC
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	//	fmt.Printf("收到心跳!	%d号任务(term:%d)收到来自%d号任务(term:%d)的心跳!,\n", rf.me, rf.term, args.LeaderId, args.Term)
	//情况1: 收到的rpc的term太旧
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false

	} else {
		//情况2: 收到的rpc的term 比自己的term大或相等
		rf.status = "follower"
		rf.term = args.Term

		reply.Success = true
		rf.term = args.Term

		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.timer.Reset(rf.overtime)

		rf.voteCount = 0
		rf.voteFor = -1
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for ok == false {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//情况1: 收到过期的rpc回复
	if args.Term < rf.term {
		return false
	}

	//情况2: 心跳不允许
	if reply.Success == false {
		if reply.Term > rf.term {
			rf.status = "follower"
			rf.term = reply.Term

			rf.voteFor = -1
			rf.voteCount = 0
			rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
			rf.timer.Reset(rf.overtime)
		}
	}

	fmt.Printf("心跳回复!		%d号任务给%d号任务发送心跳,结果:%v\n", rf.me, server, reply.Success)

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
	isLeader := true

	// Your code here (2B).
	//1: if this server isn't the leader, returns false.
	if rf.status != "leader" {
		return index, term, isLeader
	}

	//2: 附加
	entry := LogEntry{Command: command, Term: rf.term}
	rf.logs = append(rf.logs, entry)
	index = len(rf.logs) - 1

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
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				rf.voteFor = rf.me
				rf.voteCount = 1 //rf.voteCount++错误,率先进入第三轮选举就会被当选
				fmt.Printf("(轮数:%d)candidate!!!		第%d号任务已经进入candidate状态\n", rf.term, rf.me)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					arg := RequestVoteArgs{
						Term:         rf.term,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs),
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						arg.LastLogTerm = rf.logs[arg.LastLogIndex].Term
					}

					reply := RequestVoteReply{}
					fmt.Printf("发起投票! 	第%d号任务(term:%d)向第%d号任务发起投票\n", rf.me, rf.term, i)
					go rf.sendRequestVote(i, &arg, &reply)
				}
			case "leader":
				rf.timer.Reset(HeartBeatTimeout)
				for j, _ := range rf.peers {
					if j == rf.me {
						continue
					}

					reply := AppendEntriesReply{}
					arg := AppendEntriesArgs{
						Term: rf.term, LeaderId: rf.me}
					go rf.sendAppendEntries(j, &arg, &reply)
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
	rf.logs = make([]LogEntry, 0) //first index is 1
	rf.appliedChan = applyCh

	rf.appliedIndex = 0
	rf.committedIndex = 0

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
