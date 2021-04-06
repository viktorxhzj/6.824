package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) ShouldSnapshot(maxSize int) bool {
	return rf.persister.RaftStateSize() > maxSize
}

// Mono increasing for sure
func (rf *Raft) persistStateAndSnapshot(s Snapshot) {
	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)
}

func (rf *Raft) LastestSnapshot() Snapshot {
	data := rf.persister.ReadSnapshot()
	if len(data) == 0 {
		return Snapshot{}
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var bunk SnapshotBunk
	if err := d.Decode(&bunk); err != nil {
		panic(err)
	}
	if len(bunk.Entries) == 0 {
		panic("无快照")
	}
	Debug(rf, "读取最新快照 %+v", bunk)
	return bunk.Entries[len(bunk.Entries)-1]
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.makeRaftStateBytes()
	rf.persister.SaveRaftState(data)
}

type SnapshotBunk struct {
	Entries []Snapshot
}

func (rf *Raft) makeSnapshotBytes(s Snapshot) []byte {
	data := rf.persister.ReadSnapshot()
	var bunk SnapshotBunk
	if len(data) >= 1 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		if err := d.Decode(&bunk); err != nil {
			panic("快照列表读取失败")
		}
	}
	// 缩进快照列表
	for len(bunk.Entries) > 0 {
		if bunk.Entries[0].LastIncludedIndex < s.LastIncludedIndex {
			bunk.Entries = bunk.Entries[1:]
		} else {
			break
		}
	}
	bunk.Entries = append(bunk.Entries, s)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(bunk)
	Debug(rf, "更新快照列表 %+v", bunk)
	return w.Bytes()
}

func (rf *Raft) makeRaftStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("BAD PERSIST")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.offset = offset
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}
