package raft

import (
	"fmt"
	"time"
)

// AppendEntriesHandler receives AppendEntries RPC
// Leader -> Follower/Candidate/Stale Leader
func (rf *Raft) AppendEntriesHandler(req *AppendEntriesRequest, resp *AppendEntriesResponse) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.info("AppendEntries RPC returns")

	rf.info("AppendEntries RPC receives %+v", *req)
	resp.ResponseTerm = rf.currentTerm

	// 1. reply false if term < currentTerm (§5.1)
	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TERM_OUTDATED
		return
	}

	// reset the election timeout
	rf.resetTrigger()

	// if RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if req.LeaderTerm > rf.currentTerm {
		rf.currentTerm = req.LeaderTerm
		rf.persist()
		rf.role = FOLLOWER
	}

	// finds the position of the given PrevLogIndex at the log
	sliceIdx := req.PrevLogIndex - rf.offset

	switch {

	// PrevLogIndex points beyond the end of the log,
	// handle it the same as if the entry exists but the term did not match
	// i.e., reply false
	case sliceIdx >= len(rf.logs):
		resp.Info = LOG_INCONSISTENT
		resp.ConflictIndex = len(rf.logs) + rf.offset - 1
		resp.ConflictTerm = -1
		return

	// PrevLogIndex matches the lastIncludedIndex (no log)
	case sliceIdx == -1 && req.PrevLogIndex == 0:

	// PrevLogIndex matches the lastIncludedIndex in the snapshot
	case sliceIdx == -1 && req.PrevLogIndex == rf.lastIncludedIndex:

	case sliceIdx < 0:
		resp.Info = LOG_INCONSISTENT
		resp.ConflictIndex = 0
		resp.ConflictTerm = -1
		msg := fmt.Sprintf("%s A=%d,C=%d,T=%d,O=%d,{...=>[%d|%d]}",
			time.Now().Format("15:04:05.000"), rf.lastAppliedIndex, rf.commitIndex, rf.currentTerm, rf.offset, rf.lastIncludedIndex, rf.lastIncludedTerm)

		if len(rf.logs) == 0 {
			msg += "{} "
		} else {
			msg += fmt.Sprintf("{%+v->%+v} ", rf.logs[0], rf.logs[len(rf.logs)-1])
		}
		msg += fmt.Sprintf(RAFT_FORMAT, rf.me)
		msg += fmt.Sprintf("##### APPEND_ENTRIES REQ3%+v", *req)
		msg += "\n"

		fmt.Println(msg)
		return

	default:
		// 2. reply false if the log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		if rf.logs[sliceIdx].Term != req.PrevLogTerm {
			resp.ConflictTerm = rf.logs[sliceIdx].Term
			for i := 0; i <= sliceIdx; i++ {
				if rf.logs[i].Term == resp.ConflictTerm {
					resp.ConflictIndex = rf.logs[i].Index
					break
				}
			}

			resp.Info = LOG_INCONSISTENT
			return
		}
	}

	resp.Info = SUCCESS

	// 3. if an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. append any new entries not already in the log
	i := sliceIdx + 1
	j := 0

	es := make([]LogEntry, len(req.Entries))
	copy(es, req.Entries)
	for j < len(es) {
		if i == len(rf.logs) {
			rf.logs = append(rf.logs, es[j])
		} else if rf.logs[i].Term != es[j].Term {
			rf.logs = rf.logs[:i]
			rf.logs = append(rf.logs, es[j])
		}
		i++
		j++
	}
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	rf.receiverTryUpdateCommitIndex(req)
	/*--------------------CRITICAL SECTION--------------------*/
}

func (rf *Raft) sendAppendEntries(server int) {
outer:
	for !rf.killed() {

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		// if last log index ≥ nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Index < rf.nextIndex[server] {
			rf.mu.Unlock()
			return
		}
		if len(rf.logs) == 0 && rf.lastIncludedIndex < rf.nextIndex[server] {
			rf.mu.Unlock()
			return
		}

		// if needs snapshot, send InstallSnapshot RPC first
		// then retry AppendEntries RPC

		// prevLogIndex always >= 0
		prevLogIndex := rf.nextIndex[server] - 1
		pos := prevLogIndex - rf.offset

		// when the log is empty and the PrevLogIndex matches the lastIncludedIndex,
		// there is no need to send AppendEntries RPC, and this situation is already excluded above
		// Therefore, here needSnapshot is a must

		// if needs snapshot, send InstallSnapshot RPC first
		// then retry AppendEntries RPC
		if needSnapshot := (len(rf.logs) == 0) || (pos < -1); needSnapshot {
			rf.info(RAFT_FORMAT+"需要快照，因为next=%+v", server, rf.nextIndex)
			var snapReq InstallSnapshotRequest
			var snapResp InstallSnapshotResponse

			snapReq.LeaderId = rf.me
			snapReq.LeaderTerm = rf.currentTerm
			snapReq.Snapshot = rf.lastestSnapshot()
			rf.mu.Unlock()
			/*--------------------CRITICAL SECTION--------------------*/

			rf.peers[server].Call("Raft.InstallSnapshotHandler", &snapReq, &snapResp)

			/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
			rf.mu.Lock()
			// if the node is no longer a Leader,
			// there is no need to deal with snapshot/append entries anymore
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}

			switch snapResp.Info {
			// upon success, it is guaranteed that the Follower is sync-ed with
			// the Leader up to the lastIncludedIndex of the snapshot
			// here we set the nextIndex in a pessimistic view
			case SUCCESS:
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.info("快照RPC->"+RAFT_FORMAT+"成功，更新next=%+v", server, rf.nextIndex)
				rf.mu.Unlock()
				continue outer

			// term is out of date, steps down immediately
			case TERM_OUTDATED:
				rf.role = FOLLOWER
				rf.currentTerm = snapResp.ResponseTerm
				rf.info("快照RPC->"+RAFT_FORMAT+"返回TermOutdated，更新Term %d->%d", server, rf.currentTerm, snapResp.ResponseTerm)
				rf.persist()
				rf.mu.Unlock()
				return

			// upon network_failure, retry AppendEntries RPC will still
			// end up retrying InstallSnapshot RPC
			default:
				rf.info("快照RPC->"+RAFT_FORMAT+"网络波动", server)
				rf.mu.Unlock()
				continue outer
			}
			/*--------------------CRITICAL SECTION--------------------*/
		}

		// From now on, it is assured that snapshot is not needed
		var req AppendEntriesRequest
		var resp AppendEntriesResponse

		if pos == -1 {
			req.PrevLogTerm = rf.lastIncludedTerm
		} else {
			req.PrevLogTerm = rf.logs[pos].Term
		}
		req.PrevLogIndex = prevLogIndex
		req.Entries = make([]LogEntry, len(rf.logs[pos+1:]))
		copy(req.Entries, rf.logs[pos+1:])
		req.LeaderTerm = rf.currentTerm
		req.LeaderId = rf.me
		req.LeaderCommitIndex = rf.commitIndex

		rf.info("追加RPC->"+RAFT_FORMAT+"%+v", server, req)
		rf.mu.Unlock()
		/*--------------------CRITICAL SECTION--------------------*/

		rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp)

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		rf.mu.Lock()

		// if the node is no longer a Leader,
		// there is no need to deal with snapshot/append entries anymore
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}

		switch resp.Info {

		// upon success, we update matchIndex/nextIndex for the Follower
		case SUCCESS:
			// ensure that matchIndex doesn't go backwards
			if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
				rf.matchIndex[server] = n
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.info("追加RPC->"+RAFT_FORMAT+"成功,更新match=%+v,next=%+v", server, rf.matchIndex, rf.nextIndex)
			rf.leaderTryUpdateCommitIndex()
			rf.mu.Unlock()
			return

		// term is out of date, steps down immediately
		case TERM_OUTDATED:
			rf.role = FOLLOWER
			rf.currentTerm = resp.ResponseTerm
			rf.info("追加RPC->"+RAFT_FORMAT+"返回TermOutdated, 更新Term %d->%d", server, rf.currentTerm, resp.ResponseTerm)
			rf.persist()
			rf.mu.Unlock()
			return

		// we need to reduce PrevLogIndex to ultimately find the matching entry
		case LOG_INCONSISTENT:
			if resp.ConflictIndex == 0 {
				rf.nextIndex[server] = 1
			}

			idx := rf.searchRightIndex(resp.ConflictTerm)
			// upon receiving a conflict response, the Leader should first search its log for conflictTerm
			// if it finds an entry in its log with that term, it should set nextIndex to be the one
			// beyond the index of the last entry in that term in its log
			// if it does not find an entry with that term, it should set nextIndex = conflictIndex
			if len(rf.logs) == 0 {
				rf.nextIndex[server] = resp.ConflictIndex
			} else if rf.logs[idx].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx].Index + 1
			} else if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx-1].Index + 1
			} else {
				rf.nextIndex[server] = resp.ConflictIndex
			}
			rf.info("追加RPC与"+RAFT_FORMAT+"日志不一致, ConflictIdx=%d, ConflictTerm=%d, 更新next=%+v", server, resp.ConflictIndex, resp.ConflictTerm, rf.nextIndex)
			rf.mu.Unlock()

		// upon network_failure, retry AppendEntries RPC
		default:
			rf.info("追加RPC->"+RAFT_FORMAT+"网络波动", server)
			rf.mu.Unlock()
		}
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (rf *Raft) sendHeartBeat(server int) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	pos := prevLogIndex - rf.offset

	if needSnapshot := pos < -1; needSnapshot {
		rf.info(RAFT_FORMAT+"需要快照，因为next=%+v", server, rf.nextIndex)
		var snapReq InstallSnapshotRequest
		var snapResp InstallSnapshotResponse

		snapReq.LeaderId = rf.me
		snapReq.LeaderTerm = rf.currentTerm
		snapReq.Snapshot = rf.lastestSnapshot()
		rf.mu.Unlock()
		/*--------------------CRITICAL SECTION--------------------*/

		rf.peers[server].Call("Raft.InstallSnapshotHandler", &snapReq, &snapResp)

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		rf.mu.Lock()
		// if the node is no longer a Leader,
		// there is no need to deal with snapshot/append entries anymore
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}

		switch snapResp.Info {
		// upon success, it is guaranteed that the Follower is sync-ed with
		// the Leader up to the lastIncludedIndex of the snapshot
		// here we set the nextIndex in a pessimistic view
		case SUCCESS:
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.info("快照RPC->"+RAFT_FORMAT+"成功，更新next=%+v", server, rf.nextIndex)
			rf.mu.Unlock()
			return

		// term is out of date, steps down immediately
		case TERM_OUTDATED:
			rf.role = FOLLOWER
			rf.currentTerm = snapResp.ResponseTerm
			rf.info("快照RPC->"+RAFT_FORMAT+"返回TermOutdated，更新Term %d->%d", server, rf.currentTerm, snapResp.ResponseTerm)
			rf.persist()
			rf.mu.Unlock()
			return

		// upon network_failure, retry AppendEntries RPC will still
		// end up retrying InstallSnapshot RPC
		default:
			rf.info("快照RPC->"+RAFT_FORMAT+"网络波动", server)
			rf.mu.Unlock()
			return
		}
		/*--------------------CRITICAL SECTION--------------------*/
	}

	// From now on, it is assured that snapshot is not needed
	var req AppendEntriesRequest
	var resp AppendEntriesResponse

	if pos == -1 {
		req.PrevLogTerm = rf.lastIncludedTerm
	} else {
		req.PrevLogTerm = rf.logs[pos].Term
	}
	req.PrevLogIndex = prevLogIndex
	req.Entries = rf.logs[pos+1:]
	req.LeaderTerm = rf.currentTerm
	req.LeaderId = rf.me
	req.LeaderCommitIndex = rf.commitIndex

	rf.info("心跳RPC->"+RAFT_FORMAT+"%+v", server, req)
	rf.mu.Unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp)

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the node is no longer a Leader,
	// there is no need to deal with snapshot/append entries anymore
	if rf.role != LEADER {
		return
	}

	switch resp.Info {

	case SUCCESS:
		// update matchIndex and nextIndex
		if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
			rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.info("心跳RPC->"+RAFT_FORMAT+"成功,更新match=%+v,next=%+v", server, rf.matchIndex, rf.nextIndex)
		rf.leaderTryUpdateCommitIndex()

	case TERM_OUTDATED:
		rf.currentTerm = resp.ResponseTerm
		rf.role = FOLLOWER
		rf.info("心跳RPC->"+RAFT_FORMAT+"返回TermOutdated, 更新Term %d->%d", server, rf.currentTerm, resp.ResponseTerm)
		rf.persist()

	case LOG_INCONSISTENT:

		if resp.ConflictIndex == 0 {
			rf.nextIndex[server] = 1
		}

		idx := rf.searchRightIndex(resp.ConflictTerm)

		if len(rf.logs) == 0 {
			rf.nextIndex[server] = resp.ConflictIndex
		} else if rf.logs[idx].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[idx].Index + 1
		} else if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[idx-1].Index + 1
		} else {
			rf.nextIndex[server] = resp.ConflictIndex
		}
		rf.info("心跳RPC与"+RAFT_FORMAT+"日志不一致, ConflictIdx=%d, ConflictTerm=%d, 更新next=%+v", server, resp.ConflictIndex, resp.ConflictTerm, rf.nextIndex)

	default:
		rf.info("追加RPC->"+RAFT_FORMAT+"网络波动", server)
	}
	/*-----------------------------------------*/
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
