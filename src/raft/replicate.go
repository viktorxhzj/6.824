package raft

import "time"

func (rf *Raft) replicateLoop() {
	for !rf.killed() {
		<-rf.appendChan

		// concurrently send RPC requests
		for i := 0; i < rf.size; i++ {
			if rf.me == i {
				continue
			}

			go rf.sendAppendEntries(i)
		}
	}
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.batchApply()
		time.Sleep(APPLY_INTERVAL)
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and logger[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) leaderTryUpdateCommitIndex() {
	// Debug(rf, "leader try commit, current commitIdx=%d, term=%d", rf.commitIndex, rf.currentTerm)
	for i := len(rf.logs) - 1; i >= 0; i-- {
		// Debug(rf, "LogEntry Index=%d, Term=%d", rf.logs[i].Index, rf.logs[i].Term)
		if rf.logs[i].Index <= rf.commitIndex {
			break
		}
		n := rf.logs[i].Index
		if rf.logs[i].Term == rf.currentTerm {
			var replicates int

			for j := 0; j < rf.size; j++ {
				if j == rf.me {
					replicates++
					continue
				}
				if rf.matchIndex[j] >= n {
					replicates++
				}
			}
			if replicates > rf.size/2 {
				rf.info("UPDATE commitIdx = %d", n)
				_ = rf.commitIndex
				rf.commitIndex = n
			}
		}
	}
}

func (rf *Raft) receiverTryUpdateCommitIndex(req *AppendEntriesRequest) {
	if req.LeaderCommitIndex > rf.commitIndex {
		_ = rf.commitIndex
		if len(rf.logs) > 0 {
			rf.commitIndex = min(req.LeaderCommitIndex, rf.logs[len(rf.logs)-1].Index)
		}
	}
}

func (rf *Raft) batchApply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastAppliedIndex {
			rf.lastAppliedIndex++
			idx := rf.lastAppliedIndex - rf.offset
			if idx < 0 {
				rf.mu.Unlock()
				continue
			}
			entry := rf.logs[idx]
			msg := ApplyMsg{
				CommandValid: entry.Command != nil,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			rf.info("应用日志 [%d|%d]", msg.CommandIndex, msg.CommandTerm)
			rf.mu.Unlock()
			rf.applyChan <- msg

		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
