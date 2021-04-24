package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

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
	currentTerm       int // init 0
	votedFor          int
	logs              []LogEntry
	offset            int
	lastIncludedIndex int
	lastIncludedTerm  int

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

// Make creates a pointer to a Raft Node.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.mu.Lock()
	// Your initialization code here (2A, 2B, 2C).
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.size = len(peers)
	rf.applyChan = applyCh
	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.appendChan = make(chan int)
	rf.offset = 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.mainLoop()
	go rf.replicateLoop()
	go rf.applyLoop()

	Debug("========" + RAFT_FORMAT + "STARTED========", rf.me)
	return rf
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.role == LEADER

	return term, isLeader
	/*-----------------------------------------*/
}

// Start tries to start agreement on the next command to be appended to Raft's log.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term := -1, -1
	var isLeader bool

	switch rf.role {
	case LEADER:

		if len(rf.logs) > 0 {
			index = rf.logs[len(rf.logs)-1].Index + 1
		} else {
			index = rf.lastIncludedIndex + 1
		}
		term = rf.currentTerm
		isLeader = true

		// local append
		rf.logs = append(rf.logs, LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		})
		rf.persist()

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.info("========LEADER RECEIVES COMMAND [%d|%d]========", index, term)
		rf.info("更新match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

		// non-blocking
		go func() { rf.appendChan <- 0 }()
	default:
		// not a leader
	}
	return index, term, isLeader
	/*--------------------CRITICAL SECTION--------------------*/
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
		case FOLLOWER:
			rf.followerLoop()
		case CANDIDATE:
			rf.candidateLoop()
		case LEADER:
			rf.leaderLoop()
		}
	}
}

func (rf *Raft) followerLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// set a new Trigger and kick off
		timeout := (TIMER_BASE + time.Duration(rand.Intn(TIMER_RANGE))) * time.Millisecond
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
			rf.info("Follower turns to Candidate, timeout=%+v", timeout)
			rf.role = CANDIDATE
			rf.trigger = nil
			rf.currentTerm++    // increment currentTerm
			rf.votedFor = rf.me // vote for self
			rf.votes = 1        // count # of votes
			rf.persist()
			rf.mu.Unlock()
			return
		default: // stays as Follower, set a new Trigger in the next round
			rf.role = FOLLOWER
			rf.trigger = nil
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) candidateLoop() {

	for !rf.killed() {

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		rf.info("start new election")
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		// set a new Trigger and kick off
		timeout := TIMER_BASE*time.Millisecond + time.Duration(rand.Intn(TIMER_RANGE))*time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "candidate timer=%+v, term=%d", timeout, rf.currentTerm)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()
		/*-----------------------------------------*/

		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendRequestVote(i, rf.trigger.StartTime)
		}

		// 并发发送 RequestVote RPCs

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		// 进入下一次循环前判断是否角色产生变化
		rf.mu.Lock()
		rf.trigger = nil
		if rf.role != CANDIDATE {
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
		if rf.role != LEADER {
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
		sleeper := time.NewTimer(HEARTBEAT_INTERVAL)
		<-sleeper.C
	}
}
