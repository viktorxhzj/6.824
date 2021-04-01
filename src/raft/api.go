package raft

import (
	"6.824/labrpc"
)

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

		index = 1
		if len(rf.logs) > 0 {
			index = rf.logs[len(rf.logs)-1].Index + 1
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
		Debug(rf, "### FOLLOWER/CANDIDATE RECEIVES COMMAND ###")
	}
	return index, term, isLeader
	/*-----------------------------------------*/
}

// RequestVoteHandler is the RPC handler for RequestVote
// Candidate to Follower/Candidate/Stale Leader
func (rf *Raft) RequestVoteHandler(req *RequestVoteRequest, resp *RequestVoteResponse) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()

	resp.ResponseTerm = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if req.CandidateTerm < rf.currentTerm {
		Debug(rf, "reject VoteRequest from %d, my term is newer", req.CandidateId)

		resp.Info = TermOutdated
		rf.mu.Unlock()
		return
	}

	lastEntry, _ := rf.lastLogInfo()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if req.CandidateTerm > rf.currentTerm {
		rf.currentTerm = req.CandidateTerm
		rf.votedFor = NoVote
		rf.persist()
		rf.role = Follower
	}

	// if already voted, reject
	if rf.votedFor != NoVote && rf.votedFor != req.CandidateId {
		Debug(rf, "reject VoteRequest from %d, already voted for %d", req.CandidateId, rf.votedFor)

		resp.Info = Rejected
		rf.mu.Unlock()
		return
	}

	// if logger is not up-to-date, reject
	if lastEntry.Term > req.LastLogTerm || (lastEntry.Term == req.LastLogTerm && lastEntry.Index > req.LastLogIndex) {
		Debug(rf, "reject VoteRequest from %d, it isn't up to date", req.CandidateId)

		resp.Info = Rejected
		rf.mu.Unlock()
		return
	}

	// if votedFor is null or candidateId, and candidate's logger is at least as up-to-date as receiver's logger, grant vote
	Debug(rf, "vote for %d, our Term=%d", req.CandidateId, req.CandidateTerm)
	rf.votedFor = req.CandidateId
	rf.persist()

	resp.Info = Granted
	rf.resetTrigger()
	rf.mu.Unlock()
	/*-----------------------------------------*/
}