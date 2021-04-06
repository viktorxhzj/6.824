package kvraft

import (
	"bytes"
	"sync"
	"time"

	"6.824/labgob"
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

	distro map[int]map[int]chan RaftResponse // distribution channels
	clerks map[int64]int64                   // sequence number for each known client
	state  map[string]string                 // state machine
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
	kv.clerks = make(map[int64]int64)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.distro = make(map[int]map[int]chan RaftResponse)

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
	Debug(kv.me, "开启通道[%d|%d]%+v {%+v}", idx, term, ch, r)
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
	rr.Key = r.Key
	rr.ClerkId = r.ClerkId
	Debug(kv.me, "关闭通道[%d|%d]%+v {%+v}", idx, term, ch, rr)

	return rr
}

func (kv *KVServer) executeLoop() {

	data :=	kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

	for !kv.killed() {

		msg := <-kv.applyCh

		// deal with snapshot
		if !msg.CommandValid && msg.SnapshotValid {
			kv.mu.Lock()
			Debug(kv.me, "收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
			kv.deserializeState(msg.Snapshot)
			ii := msg.SnapshotIndex
			for idx, v := range kv.distro {
				if idx <= ii {
					for term, vv := range v {
						vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
						delete(v, term)
					}
					delete(kv.distro, idx)
				}
			}
			kv.mu.Unlock()
			continue
		}

		// deal with commands
		idx, term := msg.CommandIndex, msg.CommandTerm
		r, ok := msg.Command.(RaftRequest)
		Debug(kv.me, "收到日志[%d|%d] {%+v}", idx, term, msg.Command)
		// No-op
		if !ok {
			kv.mu.Lock()
			mm := kv.distro[idx]
			for _, v := range mm {
				v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: r.OpType}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()
			continue
		}
		seq := kv.clerks[r.Uid]

		// 幂等性校验
		if r.OpType != GET && r.Seq <= seq {
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
			kv.mu.Lock()
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
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
			kv.mu.Lock()
			kv.state[r.Key] = r.Value
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val, OpType: PUT}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: PUT}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()

		case APPEND:
			kv.mu.Lock()
			kv.state[r.Key] = kv.state[r.Key] + r.Value
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
			mm := kv.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val, OpType: APPEND}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: APPEND}
				}
			}
			delete(kv.distro, idx)
			kv.mu.Unlock()

		}

		// should snapshot?
		if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
			kv.mu.Lock()
			snapshot := kv.serializeState()
			ii := msg.CommandIndex
			for idx, v := range kv.distro {
				if idx <= ii {
					for term, vv := range v {
						vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
						delete(v, term)
					}
					delete(kv.distro, idx)
				}
			}
			kv.rf.Snapshot(msg.CommandIndex, snapshot)
			kv.mu.Unlock()
		}

		kv.mu.Lock()
		Debug(kv.me, "Distro %+v", kv.distro)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) noopLoop() {

	for !kv.killed() {

		time.Sleep(NO_OP_INTERVAL * time.Millisecond)

		r := RaftRequest{
			OpType: NIL,
		}

		kv.result(r)
	}
}

func (kv *KVServer) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clerks)
	e.Encode(kv.state)
	return w.Bytes()
}

func (kv *KVServer) deserializeState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clerks map[int64]int64
	var state map[string]string
	if d.Decode(&clerks) != nil ||
		d.Decode(&state) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clerks = clerks
		kv.state = state
	}
}