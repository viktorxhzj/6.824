package raft

// AppendEntriesHandler is the RPC handler for AppendEntries
// Leader -> Follower/Candidate/Stale Leader
func (rf *Raft) AppendEntriesHandler(req *AppendEntriesRequest, resp *AppendEntriesResponse) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.printLog()

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
		rf.persist()
		rf.role = Follower
	}

	// 负值原文没讨论到
	// 正值表示有该index entry
	// 大于等于len表示beyond
	sliceIdx := req.PrevLogIndex - rf.offset

	switch {

	case sliceIdx >= len(rf.logs):
		// If a follower does not have prevLogIndex in its log,
		// it should return with conflictIndex = len(log) and conflictTerm = None.
		resp.Info = LogInconsistent
		resp.ConflictIndex = len(rf.logs)
		resp.ConflictTerm = -1
		rf.mu.Unlock()
		return

	case sliceIdx == -1:
		// entirely different from the beginning

	default:
		// 2. Reply false if logger doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if rf.logs[sliceIdx].Term != req.PrevLogTerm {
			resp.ConflictTerm = rf.logs[sliceIdx].Term
			for i := 0; i <= sliceIdx; i++ {
				if rf.logs[i].Term == resp.ConflictTerm {
					resp.ConflictIndex = rf.logs[i].Index
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
	// 4. Append any new entries not already in the log
	i := sliceIdx + 1
	j := 0

	for j < len(req.Entries) {
		if i == len(rf.logs) {
			rf.logs = append(rf.logs, req.Entries[j])
		} else if rf.logs[i].Term != req.Entries[j].Term {
			rf.logs = rf.logs[:i]
			rf.logs = append(rf.logs, req.Entries[j])
		}
		i++
		j++
	}
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	rf.receiverTryUpdateCommitIndex(req)
	rf.mu.Unlock()
	/*-----------------------------------------*/
}

func (rf *Raft) sendAppendEntries(server int) {
	var req AppendEntriesRequest
	for !rf.killed() {

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.logs[len(rf.logs)-1].Index >= rf.nextIndex[server] {

			prevLogIndex := rf.nextIndex[server] - 1
			rf.updateRequest2(server, &req, prevLogIndex)

			var resp AppendEntriesResponse

			Debug(rf, "AppendEntries to NODE %d, req=%+v", server, req)

			rf.mu.Unlock()
			/*-----------------------------------------*/

			// 发送RPC请求。当不OK时，说明网络异常。
			if ok := rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp); !ok {
				resp.Info = NetworkFailure
			}

			/*+++++++++++++++++++++++++++++++++++++++++*/
			rf.mu.Lock()

			// 如果已经不为Leader，终止循环
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}

			switch resp.Info {

			case Success:
				Debug(rf, "###PrevIdx=%d,Len=%d", req.PrevLogIndex, len(req.Entries))
				if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
					rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}

				Debug(rf, "AppendEntries Success, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

				rf.leaderTryUpdateCommitIndex()

				rf.mu.Unlock()
				return

			case TermOutdated:
				// term out-of-date, step down immediately
				Debug(rf, "AppendEntries TermOutdated, step down")
				rf.role = Follower
				rf.currentTerm = resp.ResponseTerm
				rf.persist()
				rf.mu.Unlock()
				return

			case LogInconsistent:
				Debug(rf, "Inconsistent with [Server %d]", server)

				// upon receiving a conflict response, the leader should first search its logger for conflictTerm.
				// if it finds an entry in its logger with that term, it should set nextIndex to be the one
				// beyond the index of the last entry in that term in its logger.
				// if it does not find an entry with that term, it should set nextIndex = conflictIndex

				// if ConflictTerm == -1
				if resp.ConflictIndex == 0 {
					rf.nextIndex[server] = 1
					rf.updateRequest1(&req, -1)
				} else {
					idx := rf.searchRightIndex(resp.ConflictTerm)

					if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
						rf.nextIndex[server] = rf.logs[idx-1].Index + 1
						rf.updateRequest1(&req, idx-1)

					} else if rf.logs[idx].Term == resp.ConflictTerm {
						rf.nextIndex[server] = rf.logs[idx].Index + 1
						rf.updateRequest1(&req, idx)

					} else {
						rf.nextIndex[server] = resp.ConflictIndex
						prevLogIndex := resp.ConflictIndex - 1
						rf.updateRequest2(server, &req, prevLogIndex)
					}
				}
				rf.mu.Unlock()

			case NetworkFailure:
				// retry
				Debug(rf, "AppendEntries to %d timeout, retry", server)
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	var req AppendEntriesRequest
	var resp AppendEntriesResponse

	prevLogIndex := rf.nextIndex[server] - 1
	rf.updateRequest2(server, &req, prevLogIndex)
	Debug(rf, "HeartBeat to NODE %d, req=%+v", server, req)

	rf.mu.Unlock()
	/*-----------------------------------------*/

	// 发送RPC请求。当不OK时，说明网络异常。
	if ok := rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp); !ok {
		resp.Info = NetworkFailure
	}

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}

	switch resp.Info {

	case Success:
		// update matchIndex and nextIndex
		if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
			rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		Debug(rf, "HeartBeat Success, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

		rf.leaderTryUpdateCommitIndex()

	case TermOutdated:
		Debug(rf, "AppendEntries TermOutdated, step down")
		rf.currentTerm = resp.ResponseTerm
		rf.role = Follower
		rf.persist()

	case LogInconsistent:
		Debug(rf, "Inconsistent with [Server %d]", server)

		// if ConflictTerm == -1
		if resp.ConflictIndex == 0 {
			rf.nextIndex[server] = 1
		} else {
			idx := rf.searchRightIndex(resp.ConflictTerm)

			if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx-1].Index + 1

			} else if rf.logs[idx].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx].Index + 1

			} else {
				rf.nextIndex[server] = resp.ConflictIndex
			}
		}

	case NetworkFailure:
		Debug(rf, "Heartbeat to %d timeout", server)
	}
	/*-----------------------------------------*/
}

func (rf *Raft) updateRequest1(req *AppendEntriesRequest, idx int) {
	if idx == -1 {
		req.PrevLogIndex = 0
		req.PrevLogTerm = 0
	} else {
		req.PrevLogIndex = rf.logs[idx].Index
		req.PrevLogTerm = rf.logs[idx].Term
	}
	req.Entries = rf.logs[idx+1:]
	req.LeaderTerm = rf.currentTerm
	req.LeaderCommitIndex = rf.commitIndex
}

func (rf *Raft) updateRequest2(server int, req *AppendEntriesRequest, prevLogIndex int) {
	var prevLogTerm int
	var entries []LogEntry

	// 全量拷贝
	if prevLogIndex == 0 {
		// prevLogTerm = 0
		entries = rf.logs
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.offset].Term
		entries = rf.logs[prevLogIndex-rf.offset+1:]
	}
	req.LeaderTerm = rf.currentTerm
	req.LeaderId = rf.me
	req.PrevLogIndex = prevLogIndex
	req.PrevLogTerm = prevLogTerm
	req.Entries = entries
	req.LeaderCommitIndex = rf.commitIndex
}

func (rf *Raft) searchRightIndex(conflictTerm int) int {
	l, r := 0, len(rf.logs)-1
	// 寻找右边界
	for l < r {
		m := (l + r) / 2

		if rf.logs[m].Term == conflictTerm {
			l = m + 1
		} else if rf.logs[m].Term > conflictTerm {
			r = m - 1
		} else {
			l = m + 1
		}
	}
	return l
}
