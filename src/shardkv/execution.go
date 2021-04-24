package shardkv

import (
	"bytes"
	"strconv"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

// tryApplyAndGetResult 向raft层发起一个共识请求，并返回共识结果
// 有最大阻塞时间，保证不会永久阻塞
func (kv *ShardKV) tryApplyAndGetResult(req GeneralInput) (resp GeneralOutput) {
	idx, term, ok := kv.rf.Start(req)

	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}
	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	kv.lock("try apply" + req.OpType)

	ch := make(chan GeneralOutput, 1)
	if mm := kv.sigChans[idx]; mm == nil {
		mm = make(map[int]chan GeneralOutput)
		kv.sigChans[idx] = mm
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
		kv.removeSigChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		return
	case <-t.C:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.removeSigChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		resp.RPCInfo = APPLY_TIMEOUT
		return
	}
}

func (kv *ShardKV) executeLoop() {

main:
	for !kv.killed() {
		msg := <-kv.applyCh
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		kv.lock("execute loop")

		if msg.SnapshotValid {
			kv.tryLoadSnapshot(&msg)
			kv.unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(GeneralInput)
		kv.info("收到日志[%d|%d] %s", idx, term, r.OpType)

		switch r.OpType {
		case GET, PUT, APPEND:
			if succeed := kv.tryExecuteCommand(idx, term, &r); succeed {
				kv.trySaveSnapshot(idx, term)
			}
		case UPDATE_CONFIG:
			if succeed := kv.tryLoadConfig(&r); succeed {
				kv.mustSaveSnapshot(idx, term)
			}
		case LOAD_SHARD:
			if succeed := kv.tryLoadShard(idx, term, &r); succeed {
				kv.mustSaveSnapshot(idx, term)
			}

		case CLEAN_SHARD:
			if succeed := kv.tryCleanShard(&r); succeed {
				kv.mustSaveSnapshot(idx, term)
			}

		default:
			kv.error("invalid operation type")
		}

		kv.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (kv *ShardKV) tryLoadSnapshot(msg *raft.ApplyMsg) (succeed bool) {
	kv.lockname = "try load snapshot"
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		succeed = true
		defer kv.info("成功装载快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
		kv.deserializeState(msg.Snapshot)
		kv.cleanSigChans(msg.SnapshotIndex)
	}
	return
}

func (kv *ShardKV) tryExecuteCommand(idx, term int, r *GeneralInput) (succeed bool) {
	kv.lockname = "try execute " + r.OpType
	// 分片校验
	s := key2shard(r.Key)
	// 分片不对
	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		if ch, ok := kv.sigChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: WRONG_GROUP}
			delete(kv.sigChans[idx], term)
		}
		return
	}
	// 分片正确，但是在等待数据
	if kv.shardState[s] == RECEIVING {
		if ch, ok := kv.sigChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.sigChans[idx], term)
		}
		return
	}
	// 幂等性校验
	if r.OpType != GET && r.Seq <= kv.clientSeq[s][r.Uid] {
		if ch, ok := kv.sigChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.sigChans[idx], term)
		}
		return
	}
	succeed = true
	kv.info("成功应用%s [K:%s|V:%s]", r.OpType, r.Key, r.Value)
	kv.clientSeq[s][r.Uid] = r.Seq
	var val string
	switch r.OpType {
	case GET:
		val = kv.shardedData[s][r.Key]

	case PUT:
		kv.shardedData[s][r.Key] = r.Value
		val = kv.shardedData[s][r.Key]

	case APPEND:
		kv.shardedData[s][r.Key] = kv.shardedData[s][r.Key] + r.Value
		val = kv.shardedData[s][r.Key]
	}

	if ch, ok := kv.sigChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCESS, Value: val}
		delete(kv.sigChans[idx], term)
	}
	return
}

func (kv *ShardKV) tryLoadConfig(r *GeneralInput) (success bool) {
	kv.lockname = "try load config"
	config := r.Input.(shardctrler.Config)
	if config.Idx != kv.conf.Idx+1 {
		return
	}
	for s := range kv.shardState {
		if kv.shardState[s] != INCHARGE && kv.shardState[s] != NOTINCHARGE {
			return
		}
	}
	success = true
	defer kv.info("成功应用[Config %d]", config.Idx)
	// 现在的config成为旧的config
	var oldConfig shardctrler.Config
	shardctrler.CopyConfig(&oldConfig, &kv.conf)

	// 对初始化的特殊判断
	if oldConfig.Idx == 0 {
		for s, ngid := range config.Shards {
			if ngid == kv.gid {
				kv.shardState[s] = INCHARGE
			}
		}
		shardctrler.CopyConfig(&kv.conf, &config)
		return
	}

	for s, ngid := range config.Shards {
		switch kv.shardState[s] {
		// 已经拥有的分片
		case NOTINCHARGE:
			if ngid == kv.gid {
				kv.shardState[s] = RECEIVING
			}

		case INCHARGE:
			if ngid != kv.gid {
				kv.shardState[s] = TRANSFERRING
			}

		default:
			kv.error("invalid shardState[%d]=%d", s, kv.shardState[s])
		}
	}

	// 更新新的config
	shardctrler.CopyConfig(&kv.conf, &config)
	return
}

// tryLoadShard 尝试加载一个分片
func (kv *ShardKV) tryLoadShard(idx, term int, r *GeneralInput) (success bool) {
	kv.lockname = "try load shard"
	data := r.Input.(SingleShardData)
	s := data.Shard

	if data.ConfigIdx >= kv.conf.Idx {
		if ch, ok := kv.sigChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.sigChans[idx], term)
		}
		return
	}

	if data.ConfigIdx < kv.conf.Idx-1 || kv.shardState[s] == INCHARGE {
		if ch, ok := kv.sigChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.sigChans[idx], term)
		}
		return
	}

	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		kv.error("invalid shardState[%d]=%d", s, kv.shardState[s])
		return
	}

	success = true
	defer kv.info("成功装载分片%+v", data.SingleShardInfo)
	// 先拿来
	kv.shardedData[s] = make(map[string]string)
	for k, v := range data.State {
		kv.shardedData[s][k] = v
	}
	kv.clientSeq[s] = make(map[int64]int64)
	for k, v := range data.Clients {
		kv.clientSeq[s][k] = v
	}
	kv.shardState[s] = INCHARGE
	if ch, ok := kv.sigChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCESS}
		delete(kv.sigChans[idx], term)
	}
	return
}

func (kv *ShardKV) tryCleanShard(r *GeneralInput) (success bool) {
	kv.lockname = "try clean shard"
	data := r.Input.(SingleShardInfo)
	s := data.Shard

	if kv.shardState[s] == TRANSFERRING && data.ConfigIdx == kv.conf.Idx-1 {
		success = true
		defer kv.info("成功清理分片%+v", data)
		kv.shardState[s] = NOTINCHARGE
		kv.clientSeq[s] = make(map[int64]int64)
		kv.shardedData[s] = make(map[string]string)
	}
	return
}

func (kv *ShardKV) trySaveSnapshot(idx, term int) (succeed bool) {
	kv.lockname = "try save snapshot"
	if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
		succeed = true
		kv.mustSaveSnapshot(idx, term)
	}
	return
}

func (kv *ShardKV) mustSaveSnapshot(idx, term int) (succeed bool) {
	kv.lockname = "must save snapshot"
	if kv.maxraftstate == -1 {
		return
	}
	succeed = true
	defer kv.info("成功储存快照{...=>[%d|%d]", idx, term)
	stateBytes := kv.serializeState()
	kv.cleanSigChans(idx)
	kv.rf.Snapshot(idx, stateBytes)
	return
}

// configListenLoop 定期拉取最新的Config并应用
func (kv *ShardKV) configListenLoop() {
outer:
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		t := time.NewTimer(CONFIG_LISTEN_TIMEOUT)
		kv.lock("")
		c := kv.conf.Idx
		kv.unlock()
		ch := make(chan shardctrler.Config)
		var config shardctrler.Config
		go func(configNum int) { ch <- kv.scc.Query(configNum) }(c + 1)
		select {
		case <-t.C:
			Debug(SRV_FORMAT+"监听配置超时", kv.gid, kv.me)
			continue outer
		case config = <-ch:
		}

		// 并不是真正的可以canApply
		var canApply bool
		kv.lock("config listen")
		canApply = config.Idx == kv.conf.Idx+1
		kv.unlock()
		if canApply {
			// cp := shardctrler.Config{}
			// shardctrler.CopyConfig(&cp, &config)
			kv.rf.Start(GeneralInput{
				OpType: UPDATE_CONFIG,
				Input:  config,
			})
		}
	}
}

func (kv *ShardKV) shardOperationLoop(s int) {
outer:
	for !kv.killed() {
		time.Sleep(SHARD_OPERATION_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		kv.lock("shard operation" + strconv.Itoa(s))
		if kv.shardState[s] == TRANSFERRING {
			state := make(map[string]string)
			clients := make(map[int64]int64)
			gid := kv.conf.Shards[s]
			srvs := make([]string, len(kv.conf.Groups[gid]))
			copy(srvs, kv.conf.Groups[gid])
			for k, v := range kv.shardedData[s] {
				state[k] = v
			}
			for k, v := range kv.clientSeq[s] {
				clients[k] = v
			}
			data := SingleShardData{
				SingleShardInfo: SingleShardInfo{
					Shard:     s,
					ConfigIdx: kv.conf.Idx - 1,
					SenderGid: kv.gid,
				},
				State:   state,
				Clients: clients,
			}
			req := ReceiveShardRequest{SingleShardData: data}
			kv.unlock()
			for _, srvi := range srvs {
				var resp ReceiveShardResponse
				srv := kv.make_end(srvi)
				srv.Call("ShardKV.ReceiveShard", &req, &resp)
				if resp.RPCInfo == SUCCESS || resp.RPCInfo == DUPLICATE_REQUEST {
					r := GeneralInput{
						OpType: CLEAN_SHARD,
						Input:  data.SingleShardInfo,
					}
					kv.tryApplyAndGetResult(r)
					continue outer
				}
			}
		} else {
			kv.unlock()
		}
	}
}

func (kv *ShardKV) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSeq)
	e.Encode(kv.shardedData)
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
	var clientSeq [shardctrler.NShards]map[int64]int64
	var shardedData [shardctrler.NShards]map[string]string
	var conf shardctrler.Config
	var shardsState [shardctrler.NShards]int
	if d.Decode(&clientSeq) != nil ||
		d.Decode(&shardedData) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&shardsState) != nil {
		panic("bad deserialization")
	} else {
		kv.clientSeq = clientSeq
		kv.shardedData = shardedData
		kv.conf = conf
		kv.shardState = shardsState
	}
}

func (kv *ShardKV) removeSigChan(idx, term int) {
	kv.lock("remove distro")
	delete(kv.sigChans[idx], term)
	kv.unlock()
}

func (kv *ShardKV) cleanSigChans(uptoIdx int) {
	for idx, v := range kv.sigChans {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- GeneralOutput{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.sigChans, idx)
		}
	}
}
