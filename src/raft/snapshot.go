package raft

func (rf *Raft) InstallSnapshotHandler(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(rf, "收获快照RPC %+v", *req)
	defer Debug(rf, "快照RPC结束")

	resp.ResponseTerm = rf.currentTerm

	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TERM_OUTDATED
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

	rf.lastIncludedIndex = req.LastIncludedIndex
	rf.lastIncludedTerm = req.LastIncludedTerm
	sliIdx := rf.lastIncludedIndex - rf.offset
	rf.offset = req.LastIncludedIndex + 1
	rf.persistStateAndSnapshot(req.Snapshot)

	if sliIdx >= 0 && sliIdx < len(rf.logs)-1 && rf.logs[sliIdx].Index == rf.lastIncludedIndex && rf.logs[sliIdx].Term == rf.lastIncludedTerm {
		rf.logs = rf.logs[sliIdx+1:]
		Debug(rf, "快照没有覆盖所有日志")

	} else if sliIdx == -1 && rf.lastIncludedIndex == req.LastIncludedIndex && rf.lastIncludedTerm == req.LastIncludedTerm {
		Debug(rf, "已存在相同快照")
		return
	}
	rf.logs = []LogEntry{}
	Debug(rf, "向service发送信息")
	msg := ApplyMsg{
		CommandValid: false,
		// For 2D:
		SnapshotValid: true,
		Snapshot:      req.Snapshot.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	rf.applyChan <- msg
}

//
// CondInstallSnapshot is a service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
	// Your code here (2D).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// b := lastIncludedIndex == rf.lastIncludedIndex && lastIncludedTerm == rf.lastIncludedTerm && len(rf.logs) == 0
	// if b {
	// 	Debug(rf, "装载快照")
	// } else {
	// 	Debug(rf, "不装载快照")
	// }
	// return b
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(rf, "%03d快照完毕", index)
	Debug(rf, "%03d快照开始", index)

	// Index's position at the log
	sliIdx := index - rf.offset

	if sliIdx < 0 || sliIdx >= len(rf.logs) {
		panic("Invalid snapshot index")
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[sliIdx].Term
	rf.offset = index + 1

	s := Snapshot{
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              snapshot,
	}

	rf.logs = rf.logs[sliIdx+1:]

	rf.persistStateAndSnapshot(s)
}
