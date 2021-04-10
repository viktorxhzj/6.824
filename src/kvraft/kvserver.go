package kvraft

import (
	"bytes"
	"time"

	"6.824/labgob"
)


func (kv *KVServer) tryApplyAndGetResult(r RaftRequest) RaftResponse {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.mu.Lock()
	var idx, term int
	var ok bool
	switch r.OpType {
	case NIL:
		idx, term, ok = kv.rf.Start(nil)
	default:
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
	/*--------------------CRITICAL SECTION--------------------*/

	rr := <-ch
	close(ch)
	rr.Key = r.Key
	rr.ClerkId = r.ClerkId
	Debug(kv.me, "关闭通道[%d|%d]%+v {%+v}", idx, term, ch, rr)

	return rr
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
				for idx, v := range kv.distro {
					if idx <= ii {
						for term, vv := range v {
							vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
							delete(v, term)
						}
						delete(kv.distro, idx)
					}
				}
			}
			kv.mu.Unlock()
		} else if msg.CommandValid {
			// deal with commands
			idx, term := msg.CommandIndex, msg.CommandTerm
			r, ok := msg.Command.(RaftRequest)
			Debug(kv.me, "收到日志[%d|%d] {%+v}", idx, term, msg.Command)
			// No-op
			if !ok {
				mm := kv.distro[idx]
				for _, v := range mm {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST, OpType: r.OpType}
				}
				delete(kv.distro, idx)
				kv.mu.Unlock()
				continue main
			}
			seq := kv.clerks[r.Uid]

			// 幂等性校验
			if r.OpType != GET && r.Seq <= seq {
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
				continue main
			}

			kv.clerks[r.Uid] = r.Seq

			switch r.OpType {
			case GET:
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

			case PUT:
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

			case APPEND:
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
			}

			// should snapshot?
			if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
				stateBytes := kv.serializeState()
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
				kv.rf.Snapshot(msg.CommandIndex, stateBytes)
			}

			Debug(kv.me, "Distro %+v", kv.distro)
			kv.mu.Unlock()

		} else {
			kv.mu.Unlock()
		}
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (kv *KVServer) noopLoop() {

	for !kv.killed() {
		time.Sleep(NO_OP_INTERVAL * time.Millisecond)
		r := RaftRequest{
			OpType: NIL,
		}
		Debug(kv.me, "Periodically No-Op")
		go kv.tryApplyAndGetResult(r)
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
