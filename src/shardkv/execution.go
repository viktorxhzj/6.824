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
		case CHANGE_CONFIG:
			if succeed := kv.tryLoadConfiguration(&r); succeed {
				kv.mustSaveSnapshot(idx)
			}
		case PULL_SHARD:
			if succeed := kv.tryLoadShard(&r); succeed {
				kv.mustSaveSnapshot(idx)
			}

		case REMOVE_SHARD:
			if succeed := kv.tryCleanShard(&r); succeed {
				kv.mustSaveSnapshot(idx)
			}

		default:
			panic("invalid operation type")
		}

		kv.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
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

func (kv *ShardKV) trySaveSnapshot(lastIncludedIdx int) (succeed bool) {
	if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
		succeed = true
		kv.mustSaveSnapshot(lastIncludedIdx)
	}
	return
}

func (kv *ShardKV) mustSaveSnapshot(lastIncludedIdx int) {
	stateBytes := kv.serializeState()
	kv.cleanDistros(lastIncludedIdx)
	kv.rf.Snapshot(lastIncludedIdx, stateBytes)
}

func (kv *ShardKV) tryExecuteCommand(idx, term int, r *GeneralInput) (succeed bool) {
	// 分片校验
	s := key2shard(r.Key)
	// 分片不对
	if kv.gid != kv.conf.Shards[s] {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: WRONG_GROUP}
			delete(kv.distros[idx], term)
		}
		return
	}
	// 分片正确，但是在等待数据
	if kv.step < 0 && kv.ShardIdsToPull[s] {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.distros[idx], term)
		}
		return
	}

	// 幂等性校验
	seq := kv.clients[s][r.Uid]
	if r.OpType != GET && r.Seq <= seq {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.distros[idx], term)
		}
		return
	}

	succeed = true
	kv.clients[s][r.Uid] = r.Seq
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
	e.Encode(kv.historyClients)
	e.Encode(kv.stateMachine)
	e.Encode(kv.historyState)
	e.Encode(kv.conf)
	e.Encode(kv.prevConf)
	e.Encode(kv.step)
	e.Encode(kv.ShardIdsToPull)
	e.Encode(kv.ShardIdsToDiscard)
	return w.Bytes()
}

func (kv *ShardKV) deserializeState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clients [shardctrler.NShards]map[string]int64
	var historyClients map[int]map[int]map[string]int64
	var stateMachine [shardctrler.NShards]map[string]string
	var historyState map[int]map[int]map[string]string
	var conf shardctrler.Config
	var prevConf shardctrler.Config
	var step int
	var shardIdsToPull [shardctrler.NShards]bool
	var shardIdsToDiscard [shardctrler.NShards]bool
	if d.Decode(&clients) != nil ||
		d.Decode(&historyClients) != nil ||
		d.Decode(&stateMachine) != nil ||
		d.Decode(&historyState) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&prevConf) != nil ||
		d.Decode(&step) != nil ||
		d.Decode(&shardIdsToPull) != nil ||
		d.Decode(&shardIdsToDiscard) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clients = clients
		kv.historyClients = historyClients
		kv.stateMachine = stateMachine
		kv.historyState = historyState
		kv.conf = conf
		kv.prevConf = prevConf
		kv.step = step
		kv.ShardIdsToPull = shardIdsToPull
		kv.ShardIdsToDiscard = shardIdsToDiscard
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
