package raft

import (
	"6.824/labrpc"
)

// Make creates a pointer to a Raft Node.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		size: len(peers),
		applyChan: applyCh,
		logs: make([]LogEntry, 0),
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		appendChan: make(chan int),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.mainLoop()
	go rf.replicateLoop()

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
	defer rf.mu.Unlock()

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
		//rf.persist()

		Debug(rf, "Log after local append:%+v", rf.logs)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		// send a signal
		go func() {
			rf.appendChan <- 0
		}()

	default:
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

	Debug(rf, "VoteRequest from %d, myTerm=%d, itsTerm=%d", req.CandidateId, rf.currentTerm, req.CandidateTerm)

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

	resp.Info = Granted
	rf.resetTrigger()
	rf.mu.Unlock()
	/*-----------------------------------------*/
}

// AppendEntriesHandler is the RPC handler for AppendEntries
// Leader -> Follower/Candidate/Stale Leader
func (rf *Raft) AppendEntriesHandler(req *AppendEntriesRequest, resp *AppendEntriesResponse) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.printLog()

	Debug(rf, "\nAppendEntries from Leader\n%+v", *req)

	resp.ResponseTerm = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TermOutdated
		rf.mu.Unlock()
		return
	}

	// reset the Trigger
	rf.resetTrigger()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if req.LeaderTerm > rf.currentTerm {
		rf.currentTerm = req.LeaderTerm
		rf.role = Follower
	}

	sliceIndex := -1

	if req.PrevLogIndex != 0 {
		sliceIndex = rf.sliceIndex(req.PrevLogIndex)

	}

	switch sliceIndex {

	case -2:
		// if you get an AppendEntries RPC with a prevLogIndex 
		// that points beyond the end of your log, 
		// you should handle it the same as if you did have that entry 
		// but the term did not match (i.e., reply false).
		resp.Info = LogInconsistent
		resp.ConflictIndex = len(rf.logs)
		resp.ConflictTerm = -1
		rf.mu.Unlock()
		return

	case -1:
		// entirely different from the beginning

	default:
		// 2. Reply false if logger doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if rf.logs[sliceIndex].Term != req.PrevLogTerm {
			resp.ConflictTerm = rf.logs[sliceIndex].Term
			for i := sliceIndex; i >= 0; i-- {
				if rf.logs[i].Term == resp.ConflictTerm {
					resp.ConflictIndex = i
				} else {
					break
				}
			}

			resp.Info = LogInconsistent
			rf.mu.Unlock()
			return
		}
	}

	resp.Info = Success

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the logger
	i := sliceIndex + 1
	j := 0
	for j < len(req.Entries) && i < len(rf.logs) {
		if rf.logs[i].Term != req.Entries[j].Term {
			break
		}
		i++
		j++
	}
	if j < len(req.Entries) && i <= len(rf.logs) {
		rf.logs = append(rf.logs[:i], req.Entries[j:]...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	rf.receiverTryUpdateCommitIndex(req)
	rf.mu.Unlock()
	/*-----------------------------------------*/
}