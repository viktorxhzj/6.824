package shardctrler

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/raft"
)

const (
	CTRLER_CLIENT_PREFIX = "CTRLER-CLI "
)

var (
	CtrlerClientGlobalId int64 // monotonically increasing for convenience
)

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}


func GenerateClerkId() string {
	CtrlerClientGlobalId++
	return CTRLER_CLIENT_PREFIX + strconv.FormatInt(CtrlerClientGlobalId, 10)
}

func registerRPCs() {
	labgob.Register(JoinRequest{})
	labgob.Register(JoinResponse{})

	labgob.Register(LeaveRequest{})
	labgob.Register(LeaveResponse{})

	labgob.Register(MoveRequest{})
	labgob.Register(MoveResponse{})

	labgob.Register(QueryRequest{})
	labgob.Register(QueryResponse{})

	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})

	labgob.Register(raft.RequestVoteRequest{})
	labgob.Register(raft.RequestVoteResponse{})

	labgob.Register(RaftRequest{})
	labgob.Register(RaftResponse{})

	labgob.Register(map[int][]string{})
	labgob.Register([]int{})

	labgob.Register(Movable{})
}

func TimerForTest(c chan int) {
	var t int
	var s string
outer:
	for {
		select {
		case <-c:
			break outer
		default:
			t++
			s += "*"
			time.Sleep(time.Second)
		}
		fmt.Printf("%02d second %s\n", t, s)
		if t >= 100 {
			panic("panic_too_long")
		}
	}
}