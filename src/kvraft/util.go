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
)

var (
	KVClientGlobalId int64
)

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

func registerRPCs() {
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
