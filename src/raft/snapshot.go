package raft

func (rf *Raft) InstallSnapshotHandler(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {

	rf.mu.Lock()
	Debug(rf, "收获快照RPC %+v", *req)

	resp.ResponseTerm = rf.currentTerm

	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TERM_OUTDATED
		Debug(rf, "快照RPC对方任期过期，返回")
		rf.mu.Unlock()
		return
	}

	// reset the Trigger
	rf.resetTrigger()

	resp.Info = SUCCESS

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if req.LeaderTerm > rf.currentTerm {
		rf.currentTerm = req.LeaderTerm
		rf.persist()
		rf.role = FOLLOWER
	}

	if rf.lastIncludedIndex >= req.LastIncludedIndex {
		Debug(rf, "快照RPC过期，返回")
		rf.mu.Unlock()
		return
	}

	Debug(rf, "快照RPC向service发送信息")
	msg := ApplyMsg{
		CommandValid: false,
		// For 2D:
		SnapshotValid: true,
		Snapshot:      req.Snapshot.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	Debug(rf, "快照RPC结束")
	rf.mu.Unlock()
	rf.applyChan <- msg
}

//
// CondInstallSnapshot is a service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastIncludedIndex >= lastIncludedIndex {
		return false
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	sliIdx := rf.lastIncludedIndex - rf.offset
	rf.offset = rf.lastIncludedIndex + 1

	if sliIdx >= 0 && sliIdx < len(rf.logs)-1 {
		rf.logs = rf.logs[sliIdx+1:]
		Debug(rf, "快照没有覆盖所有日志")
	} else {
		rf.logs = []LogEntry{}
		Debug(rf, "全量快照")
	}
	s := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedIndex,
		Data:              snapshot,
	}
	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)

	rf.lastAppliedIndex = rf.lastIncludedIndex
	Debug(rf, "Raft层快照更新完毕")
	return true
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, stateBytes []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(rf, "%03d快照完毕", index)
	Debug(rf, "%03d快照开始", index)

	// Index's position at the log
	sliIdx := index - rf.offset

	if sliIdx < 0 {
		Debug(rf, "快照Index过期，无需快照")
		return
	}

	if sliIdx >= len(rf.logs) {
		panic("快照Index非法")
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[sliIdx].Term
	rf.offset = index + 1

	s := Snapshot{
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              stateBytes,
	}

	rf.logs = rf.logs[sliIdx+1:]

	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)
}
