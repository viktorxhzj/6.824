package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"sync"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m      map[int]map[int]RaftResponse
	clerks map[int64]int64
}

func (kv *KVServer) Get(req *GetRequest, resp *GetResponse) {
	// o := RaftRequest{
	// 	Key: req.Key,
	// 	ClerkId: req.ClerkId,
	// }
}

func (kv *KVServer) PutAppend(req *PutAppendRequest, resp *PutAppendResponse) {
	// Your code here.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	registerRPCs()

	kv := new(KVServer)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
