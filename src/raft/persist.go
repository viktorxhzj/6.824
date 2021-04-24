package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) ShouldSnapshot(maxSize int) bool {
	return rf.persister.RaftStateSize() > maxSize
}

func (rf *Raft) lastestSnapshot() Snapshot {
	data := rf.persister.ReadSnapshot()
	if len(data) == 0 {
		return Snapshot{}
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var s Snapshot
	if err := d.Decode(&s); err != nil {
		rf.error(err.Error())
	}
	rf.info("读取最新快照 %+v", s)
	return s
}

func (rf *Raft) LastestSnapshot() Snapshot {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastestSnapshot()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s)
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
		panic("bad deserialization")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.offset = offset
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}
