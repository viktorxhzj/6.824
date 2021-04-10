package kvraft

import (
	"sync"

	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int // snapshot if log grows this big

	distro map[int]map[int]chan RaftResponse // distribution channels
	clerks map[int64]int64                   // sequence number for each known client
	state  map[string]string                 // state machine
}

// StartKVServer must return quickly, so it should start goroutines
// for any long-running work.
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	registerRPCs()

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.state = make(map[string]string)
	kv.clerks = make(map[int64]int64)
	kv.distro = make(map[int]map[int]chan RaftResponse)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.executeLoop()
	go kv.noopLoop()

	return kv
}

func (kv *KVServer) Get(req *GetRequest, resp *GetResponse) {
	r := RaftRequest{
		Key:     req.Key,
		ClerkId: req.ClerkId,
		OpType:  GET,
	}

	rr := kv.tryApplyAndGetResult(r)
	resp.RPCInfo = rr.RPCInfo
	resp.Value = rr.Value
}

func (kv *KVServer) PutAppend(req *PutAppendRequest, resp *PutAppendResponse) {
	r := RaftRequest{
		Key:     req.Key,
		Value:   req.Value,
		ClerkId: req.ClerkId,
		OpType:  req.OpType,
	}

	rr := kv.tryApplyAndGetResult(r)
	resp.RPCInfo = rr.RPCInfo
}
