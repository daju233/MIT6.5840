package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	LeaderTerm   int       // Leader的Term
	PrevLogIndex int       // 新日志条目的上一个日志的索引
	PrevLogTerm  int       // 新日志的上一个日志的任期
	Logs         []RaftLog // 需要被保存的日志条目,可能有多个
	LeaderCommit int       // Leader已提交的最高的日志项目的索引
	LeaderId     int
}

type AppendEntriesReply struct {
	FollowerTerm int  // Follower的Term,给Leader更新自己的Term
	Success      bool // 是否推送成功
}

func (rf *Raft) StartAppendEntries(heart bool) {
	// 所有节点共享同一份request参数
	rf.resetHeartbeatTime()
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args.LeaderTerm = rf.term
	args.LeaderId = rf.me
	// 并行向其他节点发送心跳，让他们知道此刻已经有一个leader产生
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		DPrintf("我Leader%d,向%d开始发送心跳", rf.me, i)
		go rf.AppendEntries(i, heart, &args)
	}
}

// 日志+心跳处理器
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("我Follower %d,接收%d心跳!", rf.me, args.LeaderId)
	reply.Success = true
	if args.LeaderTerm < rf.term {
		reply.Success = false
		return
	}
	if args.LeaderTerm > rf.term {
		//记住对方
		rf.votedFor = args.LeaderId
		rf.term = args.LeaderTerm
	}
	DPrintf("我%d认为这是心跳时候的修改选举时间", rf.me)
	rf.resetElectionTime() //leader心跳压制选举时间
	rf.state = Follower
	reply.FollowerTerm = rf.term
}

func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTimeout = time.Now().Add(time.Duration(100) * time.Millisecond) //心跳间隔50ms
}

func (rf *Raft) AppendEntries(targetServerId int, heart bool, args *AppendEntriesArgs) {
	if heart {
		reply := AppendEntriesReply{}
		rf.sendRequestAppendEntries(targetServerId, args, &reply)
	}
}
