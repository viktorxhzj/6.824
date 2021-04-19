package kvraft

import (
	//"crypto/rand"
	//"math/big"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/raft"
)

const (
	KV_CLIENT_PREFIX = "CLI "
	LOCK_TIMEOUT     = 1000 * time.Millisecond
)

var (
	KVClientGlobalId int64
)

func (kv *KVServer) lock(namespace string) {
	kv.mu.Lock()
	kv.lockName = namespace
	kv.lockTime = time.Now()
	kv.Log("LOCK[%s]", namespace)
}

func (kv *KVServer) unlock() {
	if d := time.Since(kv.lockTime); d >= LOCK_TIMEOUT {
		panic(fmt.Sprintf("[KV %d] UNLOCK[%s] too long, cost %+v", kv.me, kv.lockName, d))
	}
	kv.mu.Unlock()
}

func GenerateClerkId() string {
	KVClientGlobalId++
	return KV_CLIENT_PREFIX + strconv.FormatInt(KVClientGlobalId, 10)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func init() {
	labgob.Register(GetRequest{})
	labgob.Register(GetResponse{})

	labgob.Register(PutAppendRequest{})
	labgob.Register(PutAppendResponse{})

	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})

	labgob.Register(raft.RequestVoteRequest{})
	labgob.Register(raft.RequestVoteResponse{})

	labgob.Register(raft.InstallSnapshotRequest{})
	labgob.Register(raft.InstallSnapshotResponse{})

	labgob.Register(RaftRequest{})
	labgob.Register(RaftResponse{})
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
