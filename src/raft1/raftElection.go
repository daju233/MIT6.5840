package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("我%d认为这是我自己选举时候的修改选举时间", rf.me)
	rf.resetElectionTime()
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.term++
	term := rf.term
	done := false
	tickets := 1
	DPrintf("%d开始选举!当前时间%v", rf.me, time.Now())
	args := RequestVoteArgs{
		SenderId:   rf.me,
		SenderTerm: rf.term,
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//拉票
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok || !reply.Voted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.term > reply.SenderTerm {
				return
			}
			tickets++
			DPrintf("我是%d,票数%d", rf.me, tickets)
			if done || tickets < len(rf.peers)/2+1 {
				return //避免重复触发心跳 leader等机制
			}
			done = true
			if rf.state != Candidate || rf.term != term { //???
				return
			}
			rf.state = Leader
			DPrintf("我有%d票,%d变成leader,任期是%d", tickets, rf.me, rf.term)
			go rf.StartAppendEntries(true)
		}(i)
	}
}

func (rf *Raft) resetElectionTime() {
	addtime := 150 + rand.Intn(151)
	rf.electionTimeout = time.Now().Add(time.Duration(addtime) * time.Millisecond) //选举间隔150-300ms
	DPrintf("我增加的超时时间是%dms,我%d下次超时时间是%v,当前任期%d", addtime, rf.me, rf.electionTimeout, rf.term)
}
