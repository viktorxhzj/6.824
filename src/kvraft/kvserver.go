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
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	distro map[int]map[int]chan RaftResponse
	clerks map[int64]int64
	state  map[string]string
}

func (kv *KVServer) Get(req *GetRequest, resp *GetResponse) {
	r := RaftRequest{
		Key:     req.Key,
		ClerkId: req.ClerkId,
		OpType:  GET,
	}

	rr := kv.result(r)

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

	rr := kv.result(r)

	resp.RPCInfo = rr.RPCInfo
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

	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.state = make(map[string]string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.distro = make(map[int]map[int]chan RaftResponse)
	kv.clerks = make(map[int64]int64)

	// You may need initialization code here.
	go kv.executeLoop()
	go kv.noopLoop()

	return kv
}

func (kv *KVServer) result(r RaftRequest) RaftResponse {

	var idx, term int
	var ok bool

	kv.mu.Lock()

	if r.OpType == NIL {
		idx, term, ok = kv.rf.Start(nil)

	} else {
		idx, term, ok = kv.rf.Start(r)
	}

	if !ok {
		kv.mu.Unlock()
		return RaftResponse{RPCInfo: WRONG_LEADER, OpType: r.OpType}
	}

	ch := make(chan RaftResponse)
	Debug(kv.me, "开启通道[%d|%d]%+v", idx, term, ch)
	if mm := kv.distro[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		kv.distro[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.mu.Unlock()

	rr := <-ch
	close(ch)

	return rr
}

func (kv *KVServer) executeLoop() {

	for !kv.killed() {

		msg := <-kv.applyCh
		if !msg.CommandValid || msg.SnapshotValid {
			continue
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r, ok := msg.Command.(RaftRequest)
		Debug(kv.me, "收到日志[%d|%d]", idx, term)
		// No-op
		if !ok {
			kv.mu.Lock()
			mm := kv.distro[idx]
			for _, v := range mm {
				v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: r.OpType}
			}
			delete(kv.distro, idx)
			// Debug(int64(kv.me), "No-Op清除idx=%d", idx)
			kv.mu.Unlock()
			continue
		}
		seq := kv.clerks[r.Uid]

		// 幂等性校验
		if r.OpType != GET && seq >= r.Seq {
			kv.mu.Lock()
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: DUPLICATE_REQUEST, OpType: r.OpType}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: r.OpType}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()
			continue
		}

		kv.clerks[r.Uid] = r.Seq

		switch r.OpType {
		case GET:
			val := kv.state[r.Key]
			kv.mu.Lock()
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val, OpType: GET}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: GET}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()

		case PUT:
			kv.state[r.Key] = r.Value
			kv.mu.Lock()
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, OpType: PUT}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: PUT}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()

		case APPEND:
			kv.state[r.Key] = kv.state[r.Key] + r.Value
			kv.mu.Lock()
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, OpType: APPEND}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: APPEND}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()

		}
	}
}

func (kv *KVServer) noopLoop() {

	for !kv.killed() {

		time.Sleep(NO_OP_INTERVAL * time.Millisecond)

		r := RaftRequest{
			OpType: NIL,
		}

		kv.result(r)
		kv.mu.Lock()
		Debug(kv.me, "Distro %+v", kv.distro)
		kv.mu.Unlock()

	}
}
