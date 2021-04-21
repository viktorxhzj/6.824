package shardkv

import (
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) configListenLoop() {
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}
		kv.lock("config listening")
		config := kv.scc.Query(-1)

		// 当且仅当为新的Config并且不在Reconfiguration时
		if config.Num == kv.conf.Num+1 && kv.step == 0 {
			kv.Log("try apply Config %d", config.Num)
			cp := shardctrler.Config{}
			shardctrler.CopyConfig(&cp, &config)
			kv.rf.Start(GeneralInput{
				OpType: CHANGE_CONFIG,
				Input:  cp,
			})
		}
		kv.unlock()
	}
}

func (kv *ShardKV) pullShard(s int) {
	kv.lock("pull shard")
	req := PullShardRequest{
		ShardInfo: ShardInfo{
			ConfigNum: kv.prevConf.Num,
			Shard: s,
		},
	}

	gid := kv.prevConf.Shards[s]
	servers := kv.prevConf.Groups[gid]
	kv.unlock()
	for _, srv := range servers {
		var resp PullShardResponse
		srvi := kv.make_end(srv)
		if ok := srvi.Call("ShardKV.PullShard", &req, &resp); !ok {
			resp.RPCInfo = NETWORK_ERROR
		}
		kv.lock("pull shard")
		// 1. 已经不是领袖，拉取了分片也没有意义
		// 2. prevConf已经更新，说明已经拉过了
		// 3. 不在Reconfiguration的过程中了
		// 4. 该分片已经拉过了
		_, leader := kv.rf.GetState()
		if !leader || kv.prevConf.Num > req.ConfigNum || kv.step == 0 || !kv.ShardIdsToPull[s] {
			kv.unlock()
			return
		}
		if resp.RPCInfo != SUCCEEDED_REQUEST {
			kv.unlock()
			continue
		}
		// 成功
		state := make(map[string]string)
		client := make(map[string]int64)
		for k, v := range resp.StateMachine {
			state[k] = v
		}
		for k, v := range resp.Clients {
			client[k] = v
		}

		// 成功的话我们就尝试Apply一下，然后直接返回。如果apply失败在下一次循环见
		kv.rf.Start(GeneralInput{
			OpType: PULL_SHARD,
			Input: PullShardData{
				ShardInfo: req.ShardInfo,
				Clients:      client,
				StateMachine: state,
			},
		})
		kv.unlock()
		return
	}
}

func (kv *ShardKV) pullShardLoop() {
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		// 如果不是Leader 没必要去拉分片
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		// 如果不在Reconfiguration过程中
		kv.lock("pull shard")
		if kv.step == 0 {
			kv.unlock()
			continue
		}

		// 对于每一个还需要拉取的分片
		for i := range kv.ShardIdsToPull {
			if kv.ShardIdsToPull[i] {
				go kv.pullShard(i)
			}
		}
		kv.unlock()
	}
}

func (kv *ShardKV) infoCleanShardLoop() {}

func (kv *ShardKV) cleanShardLoop() {}

func (kv *ShardKV) tryLoadShard(r *GeneralInput) (success bool) {
	data := r.Input.(PullShardData)

	if data.ConfigNum != kv.prevConf.Num || kv.step == 0 || !kv.ShardIdsToPull[data.Shard] {
		return
	}
	success = true
	kv.ShardIdsToPull[data.Shard] = false
	kv.step++
	kv.stateMachine[data.Shard] = data.StateMachine
	kv.clients[data.Shard] = data.Clients
	
	return
}

func (kv *ShardKV) tryCleanShard(r *GeneralInput) (success bool) {
	data := r.Input.(ShardInfo)

	if data.ConfigNum != kv.prevConf.Num || kv.step == 0 || !kv.ShardIdsToDiscard[data.Shard] {
		return
	}
	success = true
	kv.ShardIdsToDiscard[data.Shard] = false
	kv.step++
	delete(kv.historyState[data.ConfigNum], data.Shard)
	delete(kv.historyClients[data.ConfigNum], data.Shard)

	return
}

func (kv *ShardKV) tryLoadConfiguration(r *GeneralInput) (success bool) {
	config := r.Input.(shardctrler.Config)
	if config.Num != kv.conf.Num+1 && kv.step == 0 {
		kv.Log("can't apply Config %d", config.Num)
		return
	}

	// 确保没有ToPullShard和ToDiscardShard
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.ShardIdsToPull[i] || kv.ShardIdsToDiscard[i] {
			panic("not clean")
		}
	}

	success = true
	// 现在的config成为旧的config
	var oldConfig shardctrler.Config
	shardctrler.CopyConfig(&oldConfig, &kv.conf)

	for i, ngid := range config.Shards {
		pgid := oldConfig.Shards[i]
		switch {
		// 已经拥有的分片
		case pgid == kv.gid && ngid == kv.gid:

		// 需要取的分片
		case pgid != kv.gid && ngid == kv.gid:
			kv.ShardIdsToPull[i] = true
			kv.step--

		// 需要给出的分片
		case pgid == kv.gid && ngid != kv.gid:
			kv.ShardIdsToDiscard[i] = true
			des1 := make(map[string]string)
			src1 := kv.stateMachine[i]
			for k, v := range src1 {
				des1[k] = v
			}
			m1, ok := kv.historyState[oldConfig.Num]
			if !ok {
				m1 = make(map[int]map[string]string)
				kv.historyState[oldConfig.Num] = m1
			}
			m1[i] = des1
			kv.stateMachine[i] = make(map[string]string)
			des2 := make(map[string]int64)
			src2 := kv.clients[i]
			for k, v := range src2 {
				des2[k] = v
			}
			m2, ok := kv.historyClients[oldConfig.Num]
			if !ok {
				m2 = make(map[int]map[string]int64)
				kv.historyClients[oldConfig.Num] = m2
			}
			m2[i] = des2
			kv.clients[i] = make(map[string]int64)
			kv.step--
		}
	}

	// 更新新的config和旧的config
	shardctrler.CopyConfig(&kv.conf, &config)
	kv.prevConf = oldConfig
	return
}
