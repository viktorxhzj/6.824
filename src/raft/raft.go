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
	currentTerm int // init 0
	votedFor    int
	logs        []LogEntry
	offset      int
	lastIncludedIndex int
	lastIncludedTerm int

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
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.mainLoop()
	go rf.replicateLoop()
	go rf.applyLoop()

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
	isLeader = rf.role == Leader

	return term, isLeader
	/*-----------------------------------------*/
}

// Start tries to start agreement on the next command to be appended to Raft's log.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()

	index, term := -1, -1
	var isLeader bool

	switch rf.role {
	case Leader:
		Debug(rf, "### LEADER RECEIVES COMMAND ###")

		if len(rf.logs) > 0 {
			index = rf.logs[len(rf.logs)-1].Index + 1
		} else if len(rf.logs) == 0 && rf.lastIncludedIndex != -1 {
			index = rf.lastIncludedIndex + 1
		} else {
			index = 1
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

		Debug(rf, "Log after local append")
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		rf.mu.Unlock()
		// send a signal
		rf.appendChan <- 0

	default:
		rf.mu.Unlock()
	}
	return index, term, isLeader
	/*-----------------------------------------*/
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

			go rf.sendRequestVote(i, rf.trigger.StartTime)
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