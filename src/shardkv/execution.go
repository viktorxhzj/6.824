package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) executeLoop() {

	// Upon initialization
	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

main:
	for !kv.killed() {

		msg := <-kv.applyCh

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.lock("execute loop")

		kv.lastAppliedIdx = msg.CommandIndex

		if kv.tryLoadSnapshot(&msg) {
			kv.unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(GeneralInput)
		kv.Log("收到日志[%d|%d] {%+v}", idx, term, msg.Command)

		switch r.OpType {
		case GET, PUT, APPEND:
			if succeed := kv.tryExecuteCommand(idx, term, &r); succeed {
				kv.trySaveSnapshot(idx)
			}
		default:
		}

		kv.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (kv *ShardKV) configListenLoop() {
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		kv.mu.Lock()
		kv.conf = kv.scc.Query(-1)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) tryLoadSnapshot(msg *raft.ApplyMsg) (succeed bool) {
	if !msg.SnapshotValid {
		return
	}

	kv.Log("收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		succeed = true
		kv.deserializeState(msg.Snapshot)
		kv.cleanDistros(msg.SnapshotIndex)
	}
	return
}

func (kv *ShardKV) trySaveSnapshot(idx int) (succeed bool) {
	if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
		succeed = true
		stateBytes := kv.serializeState()
		kv.cleanDistros(idx)
		kv.rf.Snapshot(idx, stateBytes)
	}
	return
}

func (kv *ShardKV) tryExecuteCommand(idx, term int, r *GeneralInput) (succeed bool) {
	// 分片校验
	s := key2shard(r.Key)
	if kv.gid != kv.conf.Shards[s] {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: WRONG_GROUP}
			delete(kv.distros[idx], term)
		}
		return
	}

	// 幂等性校验
	seq := kv.clients[r.Uid]
	if r.OpType != GET && r.Seq <= seq {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.distros[idx], term)
		}
		return
	}

	succeed = true
	kv.clients[r.Uid] = r.Seq
	var val string
	switch r.OpType {
	case GET:
		if kv.stateMachine[s] != nil {
			val = kv.stateMachine[s][r.Key]
		}

	case PUT:
		if kv.stateMachine[s] == nil {
			kv.stateMachine[s] = make(map[string]string)
		}
		kv.stateMachine[s][r.Key] = r.Value
		val = kv.stateMachine[s][r.Key]
		//kv.Log("RELATED STATE=[K:%s V:%s]", r.Key, val)

	case APPEND:
		kv.stateMachine[s][r.Key] = kv.stateMachine[s][r.Key] + r.Value
		val = kv.stateMachine[s][r.Key]
		//kv.Log("RELATED STATE=[K:%s V:%s]", r.Key, val)
	}

	if ch, ok := kv.distros[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCEEDED_REQUEST, Value: val}
		delete(kv.distros[idx], term)
	}
	return
}

func (kv *ShardKV) tryApplyAndGetResult(req GeneralInput) (resp GeneralOutput) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.lock("try apply")
	idx, term, ok := kv.rf.Start(req)

	if !ok {
		kv.unlock()
		resp.RPCInfo = WRONG_LEADER
		return
	}

	ch := make(chan GeneralOutput)
	if mm := kv.distros[idx]; mm == nil {
		mm = make(map[int]chan GeneralOutput)
		kv.distros[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	t := time.NewTimer(INTERNAL_MAX_DURATION)
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
		resp.RPCInfo = INTERNAL_TIMEOUT
		return
	}
}

func (kv *ShardKV) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clients)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastAppliedIdx)
	return w.Bytes()
}

func (kv *ShardKV) deserializeState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clients map[string]int64
	var state [shardctrler.NShards]map[string]string
	var idx int
	if d.Decode(&clients) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&idx) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clients = clients
		kv.stateMachine = state
		kv.lastAppliedIdx = idx
	}
}

func (kv *ShardKV) removeDistro(idx, term int) {
	kv.lock("remove distro")
	delete(kv.distros[idx], term)
	kv.unlock()
}

func (kv *ShardKV) cleanDistros(uptoIdx int) {
	for idx, v := range kv.distros {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- GeneralOutput{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.distros, idx)
		}
	}
}
