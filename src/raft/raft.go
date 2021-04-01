package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// Raft is a Go structure implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	size      int                 // cluster size

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent status
	currentTerm int // init 0
	votedFor    int
	logs        []LogEntry
	offset      int

	// volatile status
	commitIndex      int
	lastAppliedIndex int

	// volatile status on Leader
	nextIndex  []int
	matchIndex []int

	// other states
	votes int // init 0

	// when a LogEntry is committed, it is immediately sent into applyChan
	applyChan chan ApplyMsg

	// when a LogEntry is requested, the append process is triggered by appendChan
	appendChan chan int

	// when Follower or Candidate receives AppendEntries from a valid Leader or grants a vote,
	// close the corresponding trigger and update timerStamp and timerState
	trigger *Trigger

	role int
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[NODE %d] term=%d,commitIdx=%d", rf.me, rf.currentTerm, rf.commitIndex)
}

// execute different processes based on the role of this Raft node.
func (rf *Raft) mainLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		r := rf.role
		rf.mu.Unlock()
		/*-----------------------------------------*/
		switch r {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
	}
}

func (rf *Raft) followerLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// set a new Trigger and kick off
		timeout := (TimerBase + time.Duration(rand.Intn(TimerRange))) * time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "reset timer=%+v, startTime=%+v", timeout, rf.trigger.StartTime)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()
		/*-----------------------------------------*/

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		switch {
		case rf.trigger.Elapsed: // timer naturally elapses, turns to Candidate
			Debug(rf, "Follower turns to Candidate, timeout=%+v", timeout)
			rf.role = Candidate
			rf.trigger = nil
			rf.currentTerm++    // increment currentTerm
			rf.votedFor = rf.me // vote for self
			rf.votes = 1        // count # of votes
			rf.persist()
			rf.mu.Unlock()
			return
		default: // stays as Follower, set a new Trigger in the next round
			rf.role = Follower
			rf.trigger = nil
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) candidateLoop() {

	for !rf.killed() {
		Debug(rf, "start new election")

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}

		// set a new Trigger and kick off
		timeout := TimerBase*time.Millisecond + time.Duration(rand.Intn(TimerRange))*time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "candidate timer=%+v, term=%d", timeout, rf.currentTerm)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()
		/*-----------------------------------------*/

		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendRequestVoteV2(i, rf.trigger.StartTime)
		}

		// 并发发送 RequestVote RPCs
		
		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		// 进入下一次循环前判断是否角色产生变化
		rf.mu.Lock()
		rf.trigger = nil
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++    // increment currentTerm
		rf.votedFor = rf.me // vote for self
		rf.votes = 1        // count # of votes
		rf.persist()
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) leaderLoop() {

	// periodically send heartbeats
	/*-----------------------------------------*/
	for !rf.killed() {

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/

		// concurrently send heartbeats
		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendHeartBeat(i)
		}

		// after sending heartbeats, sleep Leader for a while
		sleeper := time.NewTimer(HeartBeatInterval * time.Millisecond)
		<-sleeper.C
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil {
		panic("BAD PERSIST")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.offset = offset
	}
}

//
// CondInstallSnapshot is a service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// sendRequestVote 发送RPC请求，并处理RPC返回
// 该函数为异步调用
func (rf *Raft) sendRequestVote(server int, req RequestVoteRequest, st int64) {
	var resp RequestVoteResponse

	// 发送RPC请求。当不OK时，说明网络异常。
	if ok := rf.peers[server].Call("Raft.RequestVoteHandler", &req, &resp); !ok {
		resp.Info = NetworkFailure
	}

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 在RPC返回时，有可能已经退回到Follower或者获选为Leader了。
	if rf.role != Candidate {
		return
	}

	switch resp.Info {
	case Granted: // 获得投票

		// 在获得这张投票时，自己的任期已经更新了，则选票无效。
		if rf.currentTerm != req.CandidateTerm {
			return
		}

		rf.votes++

		if rf.votes > rf.size/2 { // 获得当前任期的大多数选票
			// 已经成为Leader了
			if rf.role == Leader {
				return
			}

			// 获选Leader
			rf.role = Leader
			Debug(rf, "#####LEADER ELECTED! votes=%d, Term=%d#####", rf.votes, rf.currentTerm)

			// reinitialize volatile status after election
			// 空日志返回索引0
			entry, _ := rf.lastLogInfo()
			lastLogIndex := entry.Index

			// 当选时，自动填充一个空 LogEntry
			// lastLogIndex++
			// rf.logs = append(rf.logs, LogEntry{
			// 	Index: lastLogIndex,
			// 	Term:  rf.currentTerm,
			// })
			// rf.persist()

			for i := 0; i < rf.size; i++ {
				// nextIndex[]: initialize to leader last logger index + 1
				// 初始化为1，最小不会小于1
				rf.nextIndex[i] = lastLogIndex + 1

				// matchIndex[]: initialize to 0
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = lastLogIndex

			Debug(rf, "Upon election, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

			// 结束定时器
			rf.closeTrigger(st)

		}

	case TermOutdated: // 发送RPC时的任期过期
		// 有可能现在的任期是最新的
		if rf.currentTerm >= resp.ResponseTerm {
			return
		}

		// 更新任期，回退Follower
		rf.currentTerm = resp.ResponseTerm
		rf.role = Follower
		rf.persist()
		Debug(rf, "term is out of date and roll back, %d<%d", rf.currentTerm, resp.ResponseTerm)

		// 结束定时器
		rf.closeTrigger(st)

	case Rejected:
		Debug(rf, "VoteRequest to server %d is rejected", server)

	case NetworkFailure:
		Debug(rf, "VoteRequest to server %d timeout", server)
	}
	/*-----------------------------------------*/
}
