package kvraft

import (
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	lockName string
	lockTime time.Time

	maxraftstate int // snapshot if log grows this big
	lastIdx int

	distros map[int]map[int]chan RaftResponse // distribution channels
	clients map[string]int64                   // sequence number for each known client
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
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.state = make(map[string]string)
	kv.clients = make(map[string]int64)
	kv.distros = make(map[int]map[int]chan RaftResponse)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.executeLoop()

	return kv
}

func (kv *KVServer) Get(args *GetRequest, reply *GetResponse) {
	defer func() {
		kv.Debug("Get RPC returns, %+v", *reply)
	}()
	req := RaftRequest{
		Key:     args.Key,
		ClerkId: args.ClerkId,
		OpType:  GET,
	}
	
	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.Value = resp.Value
	reply.RPCInfo = resp.RPCInfo
}

func (kv *KVServer) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	defer func() {
		kv.Debug("PutAppend RPC returns, %+v", *reply)
	}()
	req := RaftRequest{
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		OpType:  args.OpType,
	}
	
	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.OpType = args.OpType
	reply.Value = resp.Value
	reply.RPCInfo = resp.RPCInfo
}
