package raft

// RequestVoteHandler is the RPC handler for RequestVote
// Candidate to Follower/Candidate/Stale Leader
func (rf *Raft) RequestVoteHandler(req *RequestVoteRequest, resp *RequestVoteResponse) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.info("RequestVote RPC returns")
	rf.info("RequestVote RPC receives %+v", *req)

	resp.ResponseTerm = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if req.CandidateTerm < rf.currentTerm {
		rf.info("reject VoteRequest from %d, my term is newer", req.CandidateId)

		resp.Info = TERM_OUTDATED
		return
	}

	var lastIndex, lastTerm int
	// 获取最后一个log的信息
	if len(rf.logs) != 0 {
		lastIndex, lastTerm = rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	} else {
		lastIndex, lastTerm = rf.lastIncludedIndex, rf.lastIncludedTerm
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if req.CandidateTerm > rf.currentTerm {
		rf.currentTerm = req.CandidateTerm
		rf.votedFor = -1
		rf.persist()
		rf.role = FOLLOWER
	}

	// if already voted, reject
	if rf.votedFor != -1 && rf.votedFor != req.CandidateId {
		rf.info("reject VoteRequest from %d, already voted for %d", req.CandidateId, rf.votedFor)

		resp.Info = VOTE_REJECTED
		return
	}

	// if logger is not up-to-date, reject
	if lastTerm > req.LastLogTerm || (lastTerm == req.LastLogTerm && lastIndex > req.LastLogIndex) {
		rf.info("reject VoteRequest from %d, it isn't up to date", req.CandidateId)

		resp.Info = VOTE_REJECTED
		return
	}

	// if votedFor is null or candidateId, and candidate's logger is at least as up-to-date as receiver's logger, grant vote
	rf.info("vote for %d, our Term=%d", req.CandidateId, req.CandidateTerm)
	rf.votedFor = req.CandidateId
	rf.persist()

	resp.Info = VOTE_GRANTED
	rf.resetTrigger()
	/*-----------------------------------------*/
}

func (rf *Raft) sendRequestVote(server int, st int64) {

	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}

	var req RequestVoteRequest
	var resp RequestVoteResponse

	var lastIndex, lastTerm int
	// 获取最后一个log的信息
	if len(rf.logs) != 0 {
		lastIndex, lastTerm = rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	} else {
		lastIndex, lastTerm = rf.lastIncludedIndex, rf.lastIncludedTerm
	}

	req = RequestVoteRequest{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  lastIndex,
		LastLogTerm:   lastTerm,
	}

	rf.mu.Unlock()

	// 发送RPC请求。当不OK时，说明网络异常。
	rf.peers[server].Call("Raft.RequestVoteHandler", &req, &resp)

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 在RPC返回时，有可能已经退回到Follower或者获选为Leader了。
	if rf.role != CANDIDATE {
		return
	}

	switch resp.Info {
	case VOTE_GRANTED: // 获得投票

		// 在获得这张投票时，自己的任期已经更新了，则选票无效。
		if rf.currentTerm != req.CandidateTerm {
			return
		}

		rf.votes++

		if rf.votes > rf.size/2 { // 获得当前任期的大多数选票
			// 已经成为Leader了
			if rf.role == LEADER {
				return
			}

			// 获选Leader
			rf.role = LEADER
			rf.info("========LEADER ELECTED! votes=%d, Term=%d========", rf.votes, rf.currentTerm)

			// reinitialize volatile status after election
			// 空日志返回索引0
			var lastLogIndex int

			if len(rf.logs) != 0 {
				lastLogIndex = rf.logs[len(rf.logs)-1].Index
			} else {
				lastLogIndex = rf.lastIncludedIndex
			}

			for i := 0; i < rf.size; i++ {
				// nextIndex[]: initialize to leader last logger index + 1
				// 初始化为1
				rf.nextIndex[i] = lastLogIndex + 1

				// matchIndex[]: initialize to 0
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = lastLogIndex

			rf.info("Upon election, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

			// 结束定时器
			rf.closeTrigger(st)

		}

	case TERM_OUTDATED: // 发送RPC时的任期过期
		// 有可能现在的任期是最新的
		if rf.currentTerm >= resp.ResponseTerm {
			return
		}

		// 更新任期，回退Follower
		rf.currentTerm = resp.ResponseTerm
		rf.role = FOLLOWER
		rf.persist()
		rf.info("term is out of date and roll back, %d<%d", rf.currentTerm, resp.ResponseTerm)

		// 结束定时器
		rf.closeTrigger(st)

	case VOTE_REJECTED:
		rf.info("VoteRequest to server %d is rejected", server)

	default:
		rf.info("VoteRequest to server %d timeout", server)
	}
	/*-----------------------------------------*/
}
