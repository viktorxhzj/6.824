package raft

import (
	"fmt"
	"time"
)

type (
	RPCInfo int
	MsgType int
)

const (
	TIMER_BASE         = 200
	TIMER_RANGE        = 300
	HEARTBEAT_INTERVAL = 100
	APPLY_INTERVAL     = 100 * time.Millisecond

	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	// Common
	TERM_OUTDATED   RPCInfo = 0
	NETWORK_FAILURE RPCInfo = 1
	// AppendEntries
	SUCCESS          RPCInfo = 2
	LOG_INCONSISTENT RPCInfo = 3
	// RequestVote
	VOTE_GRANTED  RPCInfo = 4
	VOTE_REJECTED RPCInfo = 5
)

//
// used as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// RequestVoteRequest is a RequestVote RPC request structure.
type RequestVoteRequest struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// RequestVoteResponse is a RequestVote RPC response structure.
type RequestVoteResponse struct {
	// Your data here (2A).
	ResponseId   int
	ResponseTerm int
	Info         RPCInfo
}

// AppendEntriesRequest is a AppendEntries RPC request structure.
type AppendEntriesRequest struct {
	LeaderTerm        int
	LeaderId          int
	LeaderCommitIndex int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
}

func (r AppendEntriesRequest) String() string {
	s1 := fmt.Sprintf("Prev=[%d|%d],", r.PrevLogIndex, r.PrevLogTerm)
	if len(r.Entries) == 0 {
		return s1 + "{}"
	} else {
		return s1 + fmt.Sprintf("{%+v->%+v}", r.Entries[0], r.Entries[len(r.Entries)-1])
	}
}

// AppendEntriesResponse is a AppendEntries RPC response structure.
type AppendEntriesResponse struct {
	ResponseId    int
	ResponseTerm  int
	ConflictIndex int
	ConflictTerm  int
	Info          RPCInfo
}

func (r AppendEntriesResponse) String() string {
	return fmt.Sprintf("RespTerm=%d,ConIdx=%d,ConTerm=%d", r.ResponseTerm, r.ConflictIndex, r.ConflictTerm)
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l LogEntry) String() string {
	return fmt.Sprintf("[%d|%d]", l.Index, l.Term)
}

type Snapshot struct {
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (s Snapshot) String() string {
	return fmt.Sprintf("{SNAPSHOT=>[%d|%d]}", s.LastIncludedIndex, s.LastIncludedTerm)
}

type InstallSnapshotRequest struct {
	LeaderTerm int
	LeaderId   int
	Snapshot
}

type InstallSnapshotResponse struct {
	ResponseId   int
	ResponseTerm int
	Info         RPCInfo
}
