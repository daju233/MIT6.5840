package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type raftState int

const (
	Follower  raftState = iota // 0
	Candidate                  // 1
	Leader                     // 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	id            int
	state         raftState     //当前状态
	lastHeartBeat time.Time     //上次心跳时间
	overtime      time.Duration //超时时间
	currentTerm   int           //最新任期
	votedFor      int           //本任期内获得投票的peer
	logs          []*RaftLog    //log合集
	commitIndex   int           //已提交的最新日志的索引
	lastApplied   int           //最新索引
	nextIndex     []int         //for leader 发送给每个服务器的下一个log的index
	matchIndex    []int         //for leader 每个服务器中的最高日志

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type RaftLog struct {
	Term int
}

// 添加日志/心跳
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []*RaftLog
}

type AppendEntriesReply struct {
	Isvoted bool
}

func (rf *Raft) AppendHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// isleader =
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// server := args.ServerId
	// println(server)
	// rf.sendRequestVote(server, args, reply) //向其他服务器发送投票请求
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	// lastLogIndex := len(rf.logs) - 1
	// lastLogTerm := rf.logs[lastLogIndex].Term

	reply.VoteGranted = true
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
	reply.VoteGranted = true
	return ok
}

func (rf *Raft) sendHeartBeat() {
	for {
		for _, server := range rf.peers {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.lastApplied,
			}
			reply := AppendEntriesReply{}
			server.Call("Raft.AppendHandler", &args, &reply)
		}
		time.Sleep(200 * time.Millisecond)
	}
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

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// println(rf.me)
		// Your code here (3A)
		// Check if a leader election should be started.
		// isNeedLeader := true
		if time.Since(rf.lastHeartBeat) > rf.overtime {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	tickets := 0
	rf.state = Candidate
	for i, server := range rf.peers {
		if rf.me == i {
			tickets++
			continue
		}
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// LastLogIndex: rf.logs[len(rf.logs)-1].Term,
			// LastLogTerm:  rf.logs[0].Term,
		}
		rf.resetTime()
		reply := RequestVoteReply{}
		ok := server.Call("Raft.RequestVote", &args, &reply)
		if ok {
			if reply.VoteGranted {
				tickets++
			}
		}

	}
	//因为要给自己投票，所以只要等于/2就行了
	// DPrintf("I am", rf.me, "my tickets number is", tickets)
	if tickets >= len(rf.peers)/2+1 && rf.me == 0 {
		rf.state = Leader
		rf.currentTerm++
		DPrintf("Iam %d,my tickets number is %d", rf.me, rf.state)
		go rf.sendHeartBeat()
	}
	if rf.state != Leader {
		rf.state = Follower
	}
}

// 辅助函数：生成一个随机的选举超时时间
func (rf *Raft) generateElectionTimeout() time.Duration {
	// 6.5840 实验通常建议 150ms 到 300ms
	// 但根据你的测试需要，可以适当调整范围
	min := 150
	max := 300
	// rand.Intn 是 [0, n)
	return time.Duration(min+rand.Intn(max-min+1)) * time.Millisecond
}

// 辅助函数：重置选举超时计时器
func (rf *Raft) resetTime() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	rf.overtime = rf.generateElectionTimeout()
	if rf.me == 0 {
		DPrintf("I am %d,下次超时时间%v", rf.me, rf.lastHeartBeat.Add(rf.overtime))
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.overtime = rf.generateElectionTimeout()
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// rf.overtime

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
