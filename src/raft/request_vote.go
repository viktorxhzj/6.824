package raft

func (rf *Raft) sendRequestVoteV2(server int, st int64) {

	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}

	var req RequestVoteRequest
	var resp RequestVoteResponse
	// 获取最后一个LogEntry的信息
	entry, _ := rf.lastLogInfo()

	req = RequestVoteRequest{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  entry.Index,
		LastLogTerm:   entry.Term,
	}

	rf.mu.Unlock()

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