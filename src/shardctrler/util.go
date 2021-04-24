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
	LOCK_TIMEOUT     = 5000 * time.Millisecond
)

var (
	CtrlerClientGlobalId int64 // monotonically increasing for convenience
)

func (sc *ShardCtrler) lock(namespace string) {
	sc.mu.Lock()
	sc.lockName = namespace
	sc.lockTime = time.Now()
}

func (sc *ShardCtrler) unlock() {
	if d := time.Since(sc.lockTime); d >= LOCK_TIMEOUT {
		panic(fmt.Sprintf("[KV %d] UNLOCK[%s] too long, cost %+v", sc.me, sc.lockName, d))
	} else if sc.lockName == "execute loop" {
		// fmt.Printf("QUERY COST=%+v\n", d)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// fmt.Printf("kill CTRLER %d\n", sc.me)
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

func CopyConfig(des, src *Config) {
	
	// copy num
	des.Num = src.Num
	
	// copy shards
	for i := range des.Shards {
		des.Shards[i] = src.Shards[i]
	}
	des.Groups = make(map[int][]string)

	// copy groups
	for k, v := range src.Groups {
		arr := make([]string, len(v))
		copy(arr, v)
		des.Groups[k] = arr
	}
}

func init() {
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