package kvraft

import (
	"bytes"
	"time"

	"6.824/labgob"
)


func (kv *KVServer) tryApplyAndGetResult(req RaftRequest) (resp RaftResponse) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.lock("try apply")
	var idx, term int
	var ok bool
	switch req.OpType {
	case NIL:
		idx, term, ok = kv.rf.Start(nil)
	default:
		idx, term, ok = kv.rf.Start(req)
	}

	if !ok {
		kv.unlock()
		resp.RPCInfo = WRONG_LEADER
		return
	}

	ch := make(chan RaftResponse)
	if mm := kv.distros[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		kv.distros[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	t := time.NewTimer(APPLY_TIMEOUT)
	defer t.Stop()
	select {
	case resp = <-ch:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.removeDistro(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		return
	case <-t.C:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.removeDistro(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		resp.RPCInfo = SERVER_TIMEOUT
		return
	}
}

func (kv *KVServer) removeDistro(idx, term int) {
	kv.lock("remove distro")
	delete(kv.distros[idx], term)
	kv.unlock()
}

func (kv *KVServer) executeLoop() {

	// Upon initialization
	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

main:
	for !kv.killed() {

		msg := <-kv.applyCh

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.lock("execute loop")
		kv.lastIdx = msg.CommandIndex

		// deal with snapshot
		if msg.SnapshotValid {
			kv.Log("收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.deserializeState(msg.Snapshot)
				kv.cleanDistros(msg.SnapshotIndex)
			}
			kv.unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(RaftRequest)
		kv.Log("收到日志[%d|%d] {%+v}", idx, term, msg.Command)

		// 幂等性校验
		seq := kv.clients[r.Uid]
		if r.OpType != GET && r.Seq <= seq {
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: DUPLICATE_REQUEST}
				delete(kv.distros[idx], term)
			}
			kv.unlock()
			continue main
		}

		kv.clients[r.Uid] = r.Seq

		switch r.OpType {
		case GET:
			val := kv.state[r.Key]
			//kv.Log("RELATED STATE=[K:%s V:%s]", r.Key, val)
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.distros[idx], term)
			}

		case PUT:
			kv.state[r.Key] = r.Value
			val := kv.state[r.Key]
			//kv.Log("RELATED STATE=[K:%s V:%s]", r.Key, val)
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.distros[idx], term)
			}

		case APPEND:
			kv.state[r.Key] = kv.state[r.Key] + r.Value
			val := kv.state[r.Key]
			//kv.Log("RELATED STATE=[K:%s V:%s]", r.Key, val)
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.distros[idx], term)
			}
		}

		// should snapshot?
		if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
			stateBytes := kv.serializeState()
			kv.cleanDistros(msg.CommandIndex)
			kv.rf.Snapshot(msg.CommandIndex, stateBytes)
		}

		kv.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (kv *KVServer) cleanDistros(uptoIdx int) {
	for idx, v := range kv.distros {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.distros, idx)
		}
	}
}

func (kv *KVServer) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clients)
	e.Encode(kv.state)
	e.Encode(kv.lastIdx)
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
	var idx int
	if d.Decode(&clients) != nil ||
		d.Decode(&state) != nil || 
		d.Decode(&idx) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clients = clients
		kv.state = state
		kv.lastIdx = idx
	}
	//fmt.Printf("[KV %d] Loading Snapshot, applied-idx=%d\n", kv.me, idx)
}
