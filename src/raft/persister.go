package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"sync"

	"6.824/labgob"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

type SnapshotBunk struct {
	Entries []Snapshot
}

// Mono increasing for sure
func (rf *Raft) persistStateAndSnapshot(s Snapshot) {
	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)
}

func (rf *Raft) lastestSnapshot() Snapshot {
	data := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var bunk SnapshotBunk
	if d.Decode(&bunk) != nil {
		panic("快照列表读取失败")
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
