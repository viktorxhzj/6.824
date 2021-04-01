package raft

import "fmt"

type (
	RPCInfo    int
	MsgType    int
)

const (
	TimerBase         = 200
	TimerRange        = 300
	HeartBeatInterval = 150

	Follower  = 0
	Candidate = 1
	Leader    = 2

	// Common
	TermOutdated   RPCInfo = 0
	NetworkFailure RPCInfo = 1
	// AppendEntries
	Success         RPCInfo = 2
	LogInconsistent RPCInfo = 3
	// RequestVote
	Granted  RPCInfo = 4
	Rejected RPCInfo = 5

	Apply   MsgType = 0
	NoneOp  MsgType = 1
	Persist MsgType = 2

	NoVote             = -1
	SliceIndexStart    = -1
	SliceIndexNotFound = -2
	LogIndexZero       = 0
	None               = -1
)

var (
	ZeroLogEntry LogEntry = LogEntry{Index: 0, Term: 0}
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
	s1 := fmt.Sprintf("PrevLogIdx=%d,PrevLogTerm=%d,", r.PrevLogIndex, r.PrevLogTerm)
	if len(r.Entries) == 0 {
		return s1 + "Entries={}"
	} else {
		return s1 + fmt.Sprintf("{%+v -> %+v}", r.Entries[0], r.Entries[len(r.Entries)-1])
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