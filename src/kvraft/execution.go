package kvraft

import (
	"bytes"
	"time"

	"6.824/labgob"
)

func (kv *KVServer) tryApplyAndGetResult(req *RaftRequest, resp *RaftResponse) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.mu.Lock()
	var idx, term int
	var ok bool
	switch req.OpType {
	case NIL:
		idx, term, ok = kv.rf.Start(nil)
	default:
		idx, term, ok = kv.rf.Start(*req)
	}

	if !ok {
		kv.mu.Unlock()
		resp.RPCInfo = WRONG_LEADER
		return
	}

	ch := make(chan RaftResponse)
	Debug(kv.me, "开启通道[%d|%d]%+v", idx, term, ch)
	if mm := kv.distros[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		kv.distros[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.mu.Unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	rr := <-ch
	close(ch)
	resp.Value = rr.Value
	resp.RPCInfo = rr.RPCInfo
	Debug(kv.me, "关闭通道[%d|%d]%+v", idx, term, ch)
}

func (kv *KVServer) executeLoop() {

	// Upon initialization
	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

main:
	for !kv.killed() {

		msg := <-kv.applyCh

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.mu.Lock()

		// deal with snapshot
		if msg.SnapshotValid {
			Debug(kv.me, "收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)

			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.deserializeState(msg.Snapshot)
				ii := msg.SnapshotIndex
				for idx, v := range kv.distros {
					if idx <= ii {
						for term, vv := range v {
							vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
							delete(v, term)
						}
						delete(kv.distros, idx)
					}
				}
			}
			kv.mu.Unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r, ok := msg.Command.(RaftRequest)
		Debug(kv.me, "收到日志[%d|%d] {%+v}", idx, term, msg.Command)

		// 空日志处理
		if !ok {
			mm := kv.distros[idx]
			for _, v := range mm {
				v <- RaftResponse{RPCInfo: FAILED_REQUEST}
			}
			delete(kv.distros, idx)
			kv.mu.Unlock()
			continue main
		}

		// 幂等性校验
		seq := kv.clients[r.Uid]
		if r.OpType != GET && r.Seq <= seq {
			mm := kv.distros[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: DUPLICATE_REQUEST}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}
			delete(kv.distros, idx)
			kv.mu.Unlock()
			continue main
		}

		kv.clients[r.Uid] = r.Seq

		switch r.OpType {
		case GET:
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
			mm := kv.distros[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}

		case PUT:
			kv.state[r.Key] = r.Value
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
			mm := kv.distros[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}

		case APPEND:
			kv.state[r.Key] = kv.state[r.Key] + r.Value
			val := kv.state[r.Key]
			Debug(kv.me, "RELATED STATE=[K:%s V:%s]", r.Key, val)
			mm := kv.distros[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}
		}
		delete(kv.distros, idx)

		// should snapshot?
		if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
			stateBytes := kv.serializeState()
			ii := msg.CommandIndex
			for idx, v := range kv.distros {
				if idx <= ii {
					for term, vv := range v {
						vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
						delete(v, term)
					}
					delete(kv.distros, idx)
				}
			}
			kv.rf.Snapshot(msg.CommandIndex, stateBytes)
		}

		//Debug(kv.me, "Distro %+v", kv.distro)
		kv.mu.Unlock()
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (kv *KVServer) noopLoop() {

	for !kv.killed() {
		time.Sleep(NO_OP_INTERVAL * time.Millisecond)
		req := RaftRequest{OpType: NIL}
		resp := RaftResponse{}
		Debug(kv.me, "Periodically No-Op")
		go kv.tryApplyAndGetResult(&req, &resp)
	}
}

func (kv *KVServer) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clients)
	e.Encode(kv.state)
	return w.Bytes()
}

func (kv *KVServer) deserializeState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clients map[string]int64
	var state map[string]string
	if d.Decode(&clients) != nil ||
		d.Decode(&state) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clients = clients
		kv.state = state
	}
}
