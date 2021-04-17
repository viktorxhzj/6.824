package shardkv

import (
	"strconv"
	"sync/atomic"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	KV_CLIENT_PREFIX = "KV-CLI "
)

var (
	KVClientGlobalId int64
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func GenerateClerkId() string {
	KVClientGlobalId++
	return KV_CLIENT_PREFIX + strconv.FormatInt(KVClientGlobalId, 10)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
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
	labgob.Register(shardctrler.JoinRequest{})
	labgob.Register(shardctrler.JoinResponse{})

	labgob.Register(shardctrler.LeaveRequest{})
	labgob.Register(shardctrler.LeaveResponse{})

	labgob.Register(shardctrler.MoveRequest{})
	labgob.Register(shardctrler.MoveResponse{})

	labgob.Register(shardctrler.QueryRequest{})
	labgob.Register(shardctrler.QueryResponse{})

	labgob.Register(map[int][]string{})
	labgob.Register([]int{})

	labgob.Register(shardctrler.Movable{})
}