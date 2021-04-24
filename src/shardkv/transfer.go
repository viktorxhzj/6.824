package shardkv

import (
	"strconv"
	"time"
)

// G1的config显示需要将S1发送给G2，
// 可能的返回：
// FAILED_REQUEST
// WRONG_LEADER
// SUCCEEDED_REQUEST
// DUPLICATE_REQUEST 视为成功
func (kv *ShardKV) ReceiveShard(args *ReceiveShardRequest, reply *ReceiveShardResponse) {
	defer func(t time.Time) {
		if reply.RPCInfo == APPLY_TIMEOUT {
			kv.Debug("RECEIVE RPC TIMEOUT %+v", time.Since(t))
		}
	}(time.Now())
	req := GeneralInput{
		OpType: LOAD_SHARD,
		Input:  args.ShardData,
	}
	resp := kv.tryApplyAndGetResult(req)
	reply.RPCInfo = resp.RPCInfo
}

// tryLoadShard 尝试加载一个分片
func (kv *ShardKV) tryLoadShard(idx, term int, r *GeneralInput) (success bool) {
	kv.lockname = "LOAD SHARD"
	data := r.Input.(ShardData)
	s := data.Shard

	if data.ConfigNum >= kv.conf.Num {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}

	if data.ConfigNum < kv.conf.Num-1 {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}

	if kv.shardState[s] == INCHARGE {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}

	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		panic("wrong")
	}

	success = true
	// 先拿来
	kv.state[s] = make(map[string]string)
	for k, v := range data.State {
		kv.state[s][k] = v
	}
	kv.clients[s] = make(map[string]int64)
	for k, v := range data.Clients {
		kv.clients[s][k] = v
	}
	kv.shardState[s] = INCHARGE

	if ch, ok := kv.signalChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCEEDED_REQUEST}
		delete(kv.signalChans[idx], term)
	}
	return
}

func (kv *ShardKV) shardOperationLoop(s int) {
	outer: for !kv.killed() {
		time.Sleep(SHARD_OPERATION_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		kv.lock("shard operation" + strconv.Itoa(s))
		if kv.shardState[s] == TRANSFERRING {
			state := make(map[string]string)
			clients := make(map[string]int64)
			gid := kv.conf.Shards[s]
			srvs := make([]string, len(kv.conf.Groups[gid]))
			copy(srvs, kv.conf.Groups[gid])
			for k, v := range kv.state[s] {
				state[k] = v
			}
			for k, v := range kv.clients[s] {
				clients[k] = v
			}
			data := ShardData{
				ShardInfo: ShardInfo{
					Shard:     s,
					ConfigNum: kv.conf.Num - 1,
					Gid:       kv.gid,
				},
				State:   state,
				Clients: clients,
			}
			req := ReceiveShardRequest{ShardData: data}
			kv.unlock()
			for _, srv := range srvs {
				var resp ReceiveShardResponse
				srvi := kv.make_end(srv)
				if ok := srvi.Call("ShardKV.ReceiveShard", &req, &resp); !ok {
					resp.RPCInfo = NETWORK_ERROR
				}
				if resp.RPCInfo == SUCCEEDED_REQUEST || resp.RPCInfo == DUPLICATE_REQUEST {
					r := GeneralInput{
						OpType: CLEAN_SHARD,
						Input:  data.ShardInfo,
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

func (kv *ShardKV) tryCleanShard(idx, term int, r *GeneralInput) (success bool) {
	data := r.Input.(ShardInfo)
	s := data.Shard

	if kv.shardState[s] == TRANSFERRING && data.ConfigNum == kv.conf.Num-1 {
		success = true
		kv.shardState[s] = NOTINCHARGE
		kv.clients[s] = make(map[string]int64)
		kv.state[s] = make(map[string]string)
	}
	return
}
