package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) executeLoop() {

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
		// kv.Log("收到日志[%d|%d] %s", idx, term, r.OpType)

		switch r.OpType {
		case GET, PUT, APPEND:
			if succeed := kv.tryExecuteCommand(idx, term, &r); succeed {
				kv.trySaveSnapshot(idx)
			}
		case UPDATE_CONFIG:
			if succeed := kv.tryLoadConfiguration(&r); succeed {
				kv.mustSaveSnapshot(idx)
			}
		case LOAD_SHARD:
			if succeed := kv.tryLoadShard(idx, term, &r); succeed {
				kv.mustSaveSnapshot(idx)
			}

		case CLEAN_SHARD:
			if succeed := kv.tryCleanShard(idx, term, &r); succeed {
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
	succeed = true
	kv.Log("收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.deserializeState(msg.Snapshot)
		kv.cleanSignalChans(msg.SnapshotIndex)
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
	if kv.maxraftstate == -1 {
		return
	}
	stateBytes := kv.serializeState()
	kv.cleanSignalChans(lastIncludedIdx)
	kv.rf.Snapshot(lastIncludedIdx, stateBytes)
}

func (kv *ShardKV) tryExecuteCommand(idx, term int, r *GeneralInput) (succeed bool) {
	// 分片校验
	s := key2shard(r.Key)
	// 分片不对
	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: WRONG_GROUP}
			delete(kv.signalChans[idx], term)
		}
		return
	}
	// 分片正确，但是在等待数据
	if kv.shardState[s] == RECEIVING {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}

	// 幂等性校验
	if r.OpType != GET && r.Seq <= kv.clients[s][r.Uid] {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}

	succeed = true
	kv.clients[s][r.Uid] = r.Seq
	var val string
	switch r.OpType {
	case GET:
		val = kv.state[s][r.Key]

	case PUT:
		kv.state[s][r.Key] = r.Value
		val = kv.state[s][r.Key]

	case APPEND:
		kv.state[s][r.Key] = kv.state[s][r.Key] + r.Value
		val = kv.state[s][r.Key]
	}

	if ch, ok := kv.signalChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCEEDED_REQUEST, Value: val}
		delete(kv.signalChans[idx], term)
	}
	return
}

// tryApplyAndGetResult 向raft层发起一个共识请求，并返回共识结果
// 有最大阻塞时间，保证不会永久阻塞
func (kv *ShardKV) tryApplyAndGetResult(req GeneralInput) (resp GeneralOutput) {
	idx, term, ok := kv.rf.Start(req)

	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}
	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.lock("try apply")

	ch := make(chan GeneralOutput, 1)
	if mm := kv.signalChans[idx]; mm == nil {
		mm = make(map[int]chan GeneralOutput)
		kv.signalChans[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	// 最多等待 INTERNAL_MAX_DURATION 后返回
	t := time.NewTimer(INTERNAL_TIMEOUT)
	select {
	case resp = <-ch:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.removeSignalChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		return
	case <-t.C:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.removeSignalChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		resp.RPCInfo = APPLY_TIMEOUT
		return
	}
}

func (kv *ShardKV) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clients)
	e.Encode(kv.state)
	e.Encode(kv.conf)
	e.Encode(kv.shardState)
	return w.Bytes()
}

func (kv *ShardKV) deserializeState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clients [shardctrler.NShards]map[string]int64
	var state [shardctrler.NShards]map[string]string
	var conf shardctrler.Config
	var shardsState [shardctrler.NShards]int
	if d.Decode(&clients) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&shardsState) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clients = clients
		kv.state = state
		kv.conf = conf
		kv.shardState = shardsState
	}
}

func (kv *ShardKV) removeSignalChan(idx, term int) {
	kv.lock("remove distro")
	delete(kv.signalChans[idx], term)
	kv.unlock()
}

func (kv *ShardKV) cleanSignalChans(uptoIdx int) {
	for idx, v := range kv.signalChans {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- GeneralOutput{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.signalChans, idx)
		}
	}
}
