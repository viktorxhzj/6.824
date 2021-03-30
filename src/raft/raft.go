package raft

import (
	"math/rand"
	"sync"
	"time"

	"6.824/labrpc"
)

// Raft is a Go structure implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	size      int                 // cluster size

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent status
	currentTerm int // init 0
	votedFor    int
	logs        []LogEntry

	// volatile status
	commitIndex int
	// lastAppliedIndex int

	// volatile status on Leader
	nextIndex  []int
	matchIndex []int

	// other states
	votes int // init 0

	// when a LogEntry is committed, it is immediately sent into applyChan
	applyChan chan ApplyMsg

	// when a LogEntry is requested, the append process is triggered by appendChan
	appendChan chan int

	// when Follower or Candidate receives AppendEntries from a valid Leader or grants a vote,
	// close the corresponding trigger and update timerStamp and timerState
	trigger *Trigger

	role int
}

// execute different processes based on the role of this Raft node.
func (rf *Raft) mainLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		r := rf.role
		rf.mu.Unlock()
		/*-----------------------------------------*/

		Debug(rf, "mainLoop status=%+v", r)
		switch r {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
	}
}

func (rf *Raft) followerLoop() {

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// set a new Trigger and kick off
		timeout := (TimerBase + time.Duration(rand.Intn(TimerRange))) * time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "reset timer=%+v, startTime=%+v", timeout, rf.trigger.StartTime)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()
		/*-----------------------------------------*/

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		switch {
		case rf.trigger.Elapsed: // timer naturally elapses, turns to Candidate
			Debug(rf, "Follower turns to Candidate, timeout=%+v", timeout)
			rf.role = Candidate
			rf.trigger = nil
			rf.mu.Unlock()
			return
		default: // stays as Follower, set a new Trigger in the next round
			rf.role = Follower
			rf.trigger = nil
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) candidateLoop() {

	for !rf.killed() {
		Debug(rf, "start new election")

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		rf.currentTerm++    // increment currentTerm
		rf.votedFor = rf.me // vote for self
		rf.votes = 1        // count # of votes

		// set a new Trigger and kick off
		timeout := TimerBase*time.Millisecond + time.Duration(rand.Intn(TimerRange))*time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "candidate timer=%+v, term=%d", timeout, rf.currentTerm)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		// 获取最后一个LogEntry的信息
		entry, _ := rf.lastLogInfo()

		request := RequestVoteRequest{
			CandidateTerm: rf.currentTerm,
			CandidateId:   rf.me,
			LastLogIndex:  entry.Index,
			LastLogTerm:   entry.Term,
		}

		// 并发发送 RequestVote RPCs
		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendRequestVote(i, request, rf.trigger.StartTime)
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/

		rf.trigger.Wait()

		/*+++++++++++++++++++++++++++++++++++++++++*/
		// 进入下一次循环前判断是否角色产生变化
		rf.mu.Lock()
		rf.trigger = nil
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) leaderLoop() {

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()

	// reinitialize volatile status after election

	// 空日志返回索引0
	entry, _ := rf.lastLogInfo()
	lastLogIndex := entry.Index

	// 当选时，自动填充一个空 LogEntry
	// lastLogIndex++
	// rf.logs = append(rf.logs, LogEntry{
	// 	Index: lastLogIndex,
	// 	Term:  rf.currentTerm,
	// })

	for i := 0; i < rf.size; i++ {
		// nextIndex[]: initialize to leader last logger index + 1
		rf.nextIndex[i] = lastLogIndex + 1

		// matchIndex[]: initialize to 0
		rf.matchIndex[i] = 0
	}

	rf.matchIndex[rf.me] = lastLogIndex

	Debug(rf, "Upon election, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)
	rf.mu.Unlock()
	/*-----------------------------------------*/

	for !rf.killed() {
		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// concurrently send heartbeats
		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			var entries []LogEntry
			prevLogIndex := rf.matchIndex[i] // index of logger entry immediately preceding new ones
			var prevLogTerm int

			//    |
			// [5 6 7 8 9]
			// []
			// [1]

			// 全量拷贝
			if prevLogIndex == 0 {
				entries = rf.logs
			} else {
				s := rf.logs[0].Index
				idx := prevLogIndex - s
				if idx >= len(rf.logs) { // impossible
					entries = make([]LogEntry, 0)
				} else {
					prevLogTerm = rf.logs[idx].Term
					entries = rf.logs[idx+1:]
				}
			}

			request := AppendEntriesRequest{
				LeaderTerm:        rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
				LeaderCommitIndex: rf.commitIndex,
				Entries:           entries,
			}

			go rf.sendHeartBeat(i, request)
		}

		// after sending heartbeats, sleep Leader for a while
		sleeper := time.NewTimer(HeartBeatInterval * time.Millisecond)

		rf.mu.Unlock()
		/*-----------------------------------------*/

		<-sleeper.C

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

func (rf *Raft) replicateLoop() {
	for !rf.killed() {
		<-rf.appendChan

		/*+++++++++++++++++++++++++++++++++++++++++*/
		rf.mu.Lock()

		// concurrently send RPC requests
		for i := 0; i < rf.size; i++ {
			if rf.me == i {
				continue
			}

			// If last logger index ≥ nextIndex for a follower: send
			// AppendEntries RPC with logger entries starting at nextIndex
			if rf.logs[len(rf.logs)-1].Index >= rf.nextIndex[i] {

				prevLogIndex := rf.nextIndex[i] - 1
				var prevLogTerm int
				var entries []LogEntry
				if prevLogIndex == 0 {
					entries = rf.logs
				} else {
					s := rf.logs[0].Index
					idx := prevLogIndex - s
					if idx >= len(rf.logs) { // impossible
						entries = make([]LogEntry, 0)
					} else {
						prevLogTerm = rf.logs[idx].Term
						entries = rf.logs[idx+1:]
					}
				}

				request := AppendEntriesRequest{
					LeaderTerm:        rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      prevLogIndex,
					PrevLogTerm:       prevLogTerm,
					Entries:           entries,
					LeaderCommitIndex: rf.commitIndex,
				}

				go rf.sendAppendEntries(i, request)
			}
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// CondInstallSnapshot is a service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// sendRequestVote 发送RPC请求，并处理RPC返回
// 该函数为异步调用
func (rf *Raft) sendRequestVote(server int, req RequestVoteRequest, st int64) {
	var resp RequestVoteResponse

	// 发送RPC请求。当不OK时，说明网络异常。
	if ok := rf.peers[server].Call("Raft.RequestVoteHandler", &req, &resp); !ok {
		resp.Info = NetworkFailure
	}

	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 在RPC返回时，有可能已经退回到Follower或者获选为Leader了。
	if rf.role != Candidate {
		return
	}

	switch resp.Info {
	case Granted: // 获得投票

		// 在获得这张投票时，自己的任期已经更新了，则选票无效。
		if rf.currentTerm != req.CandidateTerm {
			return
		}

		rf.votes++

		if rf.votes > rf.size/2 { // 获得当前任期的大多数选票
			// 已经成为Leader了
			if rf.role == Leader {
				return
			}

			// 获选Leader
			rf.role = Leader
			Debug(rf, "#####LEADER ELECTED! votes=%d, Term=%d#####", rf.votes, rf.currentTerm)

			// 结束定时器
			rf.closeTrigger(st)

		}

	case TermOutdated: // 发送RPC时的任期过期
		// 有可能现在的任期是最新的
		if rf.currentTerm >= resp.ResponseTerm {
			return
		}

		// 更新任期，回退Follower
		rf.currentTerm = resp.ResponseTerm
		rf.role = Follower
		Debug(rf, "term is out of date and roll back, %d<%d", rf.currentTerm, resp.ResponseTerm)

		// 结束定时器
		rf.closeTrigger(st)

	case Rejected:
		Debug(rf, "VoteRequest to server %d is rejected", server)

	case NetworkFailure:
		Debug(rf, "VoteRequest to server %d timeout", server)
	}
	/*-----------------------------------------*/
}

// sendAppendEntries keeps sending AppendEntries until success or stepping down
// 该函数为异步调用
func (rf *Raft) sendAppendEntries(server int, req AppendEntriesRequest) {
	for !rf.killed() {
		var resp AppendEntriesResponse
		Debug(rf, "\nAppendEntries to %d\nMyTerm\t%d\nMyLog\t%+v\nAppend\t%+v", server, rf.currentTerm, rf.logs, req.Entries)

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

			rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			Debug(rf, "AppendEntries Success, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

			rf.leaderTryUpdateCommitIndex()

			rf.mu.Unlock()
			return

		case TermOutdated:

			// term out-of-date, step down immediately
			Debug(rf, "AppendEntries TermOutdated, step down")
			rf.role = Follower
			rf.currentTerm = resp.ResponseTerm
			rf.mu.Unlock()
			return

		case LogInconsistent:
			Debug(rf, "Inconsistent with [Server %d], retry", server)

			// upon receiving a conflict response, the leader should first search its logger for conflictTerm.
			// if it finds an entry in its logger with that term, it should set nextIndex to be the one
			// beyond the index of the last entry in that term in its logger.
			// if it does not find an entry with that term, it should set nextIndex = conflictIndex

			l, r := 0, len(rf.logs)-1
			// 寻找右边界
			for l < r {
				m := (l + r) / 2

				if rf.logs[m].Term == resp.ConflictTerm {
					l = m + 1
				} else if rf.logs[m].Term > resp.ConflictTerm {
					r = m - 1
				} else {
					l = m + 1
				}
			}

			if rf.logs[l].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[l].Index + 1

				req.PrevLogIndex = rf.logs[l].Index
				req.PrevLogTerm = rf.logs[l].Term
				req.LeaderTerm = rf.currentTerm
				req.LeaderCommitIndex = rf.commitIndex
				req.Entries = rf.logs[l+1:]

			} else if l > 0 && rf.logs[l-1].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[l].Index + 1

				req.PrevLogIndex = rf.logs[l].Index
				req.PrevLogTerm = rf.logs[l].Term
				req.LeaderTerm = rf.currentTerm
				req.LeaderCommitIndex = rf.commitIndex
				req.Entries = rf.logs[l+1:]

			} else {
				rf.nextIndex[server] = resp.ConflictIndex

				prevLogIndex := resp.ConflictIndex - 1
				prevLogTerm := None

				sliceIndex := rf.sliceIndex(prevLogIndex)
				if sliceIndex != SliceIndexNotFound && sliceIndex != SliceIndexStart {
					prevLogTerm = rf.logs[sliceIndex].Term
				}

				req.PrevLogIndex = prevLogIndex
				req.PrevLogTerm = prevLogTerm
				req.LeaderTerm = rf.currentTerm
				req.LeaderCommitIndex = rf.commitIndex
				req.Entries = rf.logs[sliceIndex+1:]
			}

		case NetworkFailure:
			// retry
			Debug(rf, "AppendEntries to %d timeout, retry", server)
		}
		rf.mu.Unlock()
		/*-----------------------------------------*/
	}
}

// sendHeartBeat doesn't repeat
func (rf *Raft) sendHeartBeat(server int, req AppendEntriesRequest) {
	var resp AppendEntriesResponse

	Debug(rf, "\nHeartbeat to %d\nMyTerm\t%d\nMyLog\t%+v\nAppend\t%+v", server, rf.currentTerm, rf.logs, req.Entries)

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
		rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		Debug(rf, "HeartBeat Success, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)

		rf.leaderTryUpdateCommitIndex()

	case TermOutdated:
		Debug(rf, "AppendEntries TermOutdated, step down")
		rf.currentTerm = resp.ResponseTerm
		rf.role = Follower

	case LogInconsistent:
		Debug(rf, "Inconsistent with [Server %d]", server)
		l, r := 0, len(rf.logs)-1
		// 寻找右边界
		for l < r {
			m := (l + r) / 2

			if rf.logs[m].Term == resp.ConflictTerm {
				l = m + 1
			} else if rf.logs[m].Term > resp.ConflictTerm {
				r = m - 1
			} else {
				l = m + 1
			}
		}

		if rf.logs[l].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[l].Index + 1

		} else if l > 0 && rf.logs[l-1].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[l].Index + 1

		} else {
			rf.nextIndex[server] = resp.ConflictIndex
		}

	case NetworkFailure:
		Debug(rf, "Heartbeat to %d timeout", server)
		return
	}
	/*-----------------------------------------*/
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and logger[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) leaderTryUpdateCommitIndex() {
	Debug(rf, "leader try commit, current commitIdx=%d, term=%d", rf.commitIndex, rf.currentTerm)
	for i := len(rf.logs) - 1; i >= 0; i-- {
		Debug(rf, "LogEntry Index=%d, Term=%d", rf.logs[i].Index, rf.logs[i].Term)
		if rf.logs[i].Index <= rf.commitIndex {
			break
		}
		if n := rf.logs[i].Index; rf.logs[i].Term == rf.currentTerm {
			replicates := 1

			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= n {
					replicates++
				}
			}
			Debug(rf, "replicate=%d", replicates)

			if replicates > len(rf.peers)/2 {
				oldIndex := rf.commitIndex
				rf.commitIndex = n

				rf.batchCommit(oldIndex)
			}
		}
	}
}

func (rf *Raft) receiverTryUpdateCommitIndex(req *AppendEntriesRequest) {
	if req.LeaderCommitIndex > rf.commitIndex {
		oldIndex := rf.commitIndex
		if len(req.Entries) > 0 {
			rf.commitIndex = min(req.LeaderCommitIndex, req.Entries[len(req.Entries)-1].Index)
		} else {
			rf.commitIndex = req.LeaderCommitIndex
		}
		rf.batchCommit(oldIndex)
	}
}

func (rf *Raft) batchCommit(oldIndex int) {
	Debug(rf, "commit index [%d %d]", oldIndex+1, rf.commitIndex)
	for k := oldIndex + 1; k <= rf.commitIndex; k++ {
		idx := k - rf.logs[0].Index
		entry := rf.logs[idx]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}

		rf.applyChan <- msg
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
