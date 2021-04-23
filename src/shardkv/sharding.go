package shardkv

import (
	"time"

	"6.824/shardctrler"
)

// configListenLoop 定期拉取最新的Config并应用
func (kv *ShardKV) configListenLoop() {
outer:
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		t := time.NewTimer(CONFIG_LISTEN_TIMEOUT)
		ch := make(chan shardctrler.Config)
		var config shardctrler.Config
		go func() { ch <- kv.scc.Query(-1) }()
		select {
		case <-t.C:
			kv.Debug("监听配置超时")
			continue outer
		case config = <-ch:
		}

		// 并不是真正的可以canApply
		var canApply bool
		kv.lock("监听配置")
		// 什么时候可以应用新的Config？
		// 1. 新的Config更新
		// 2. 当前不在reconfig
		canApply = config.Num > kv.conf.Num && kv.step == 0
		kv.unlock()
		if canApply {
			// kv.Debug("try apply Config %d", config.Num)
			// RPC的结果的需要Deep-Copy
			cp := shardctrler.Config{}
			shardctrler.CopyConfig(&cp, &config)
			kv.rf.Start(GeneralInput{
				OpType: UPDATE_CONFIG,
				Input:  cp,
			})
		} else {
			// kv.Debug("cannot apply Config %d", config.Num)
		}
	}
}

func (kv *ShardKV) pullShardLoop() {
	for !kv.killed() {
		time.Sleep(SHARD_PULL_INTERVAL)
		// 如果不是Leader 没必要去拉分片
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		// 如果不在reconfig过程中
		kv.lock("determine whether should pull shard")
		if kv.step == 0 {
			kv.unlock()
			continue
		}

		// 对于每一个还需要拉取的分片
		for i := range kv.ShardsToPull {
			if kv.ShardsToPull[i] {
				go kv.pullShard(i)
			}
		}
		kv.unlock()
	}
}

// 对于已经拉取到的分片，通知对应集群清理
// a.ShardsToPull=true, b.ShardsToDiscard=true -> a拉取b -> a装载 -> a.ShardsToPull=false -> a.ShardsToInfoDiscard=true
// -> a通知b清理 -> b清理 -> b.ShardsToDiscard=false -> a得知b已清理 -> a.ShardsToInfoDiscard=false
func (kv *ShardKV) infoCleanShardLoop() {
	for !kv.killed() {
		time.Sleep(INFO_CLEAN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}
		kv.lock("clean shard")
		// 如何判断已经拉取的分片？
		if kv.step == 0 {
			kv.unlock()
			continue
		}

		for i := range kv.ShardsToInfoDiscard {
			if kv.ShardsToInfoDiscard[i] {
				gid := kv.oldConf.Shards[i]
				servers := kv.oldConf.Groups[gid]
				go kv.infoCleanShard(ShardInfo{Shard: i, ConfigNum: kv.oldConf.Num}, servers)
			}
		}
		kv.unlock()
	}
}

func (kv *ShardKV) pullShard(sIdx int) {
	// 其实在这一刻有可能已经不需要拉取了，但无所谓
	kv.lock("pull shard start")
	req := PullShardRequest{
		ShardInfo: ShardInfo{
			ConfigNum: kv.oldConf.Num,
			Shard:     sIdx,
		},
	}
	// 获取需要拉取分片的旧集群信息
	gid := kv.oldConf.Shards[sIdx]
	servers := kv.oldConf.Groups[gid]
	kv.unlock()
	for _, srv := range servers {
		var resp PullShardResponse
		srvi := kv.make_end(srv)
		if ok := srvi.Call("ShardKV.PullShard", &req, &resp); !ok {
			resp.RPCInfo = NETWORK_ERROR
		}
		// 1. 已经不是领袖，拉取了分片也没有意义
		// 2. oldConf已经更新，说明已经拉过了
		// 3. 不在reconfig的过程中了
		// 4. 该分片已经拉过了
		_, leader := kv.rf.GetState()
		kv.lock("pull shard each")
		if !leader || kv.oldConf.Num > req.ConfigNum || kv.step == 0 || !kv.ShardsToPull[sIdx] {
			kv.unlock()
			return
		}
		if resp.RPCInfo != SUCCEEDED_REQUEST {
			kv.unlock()
			continue
		}
		// 成功，RPC的结果的需要Deep-Copy
		state := make(map[string]string)
		client := make(map[string]int64)
		for k, v := range resp.StateMachine {
			state[k] = v
		}
		for k, v := range resp.Clients {
			client[k] = v
		}

		// 成功的话我们就尝试Apply一下，然后直接返回。如果apply失败在下一次循环见
		kv.unlock()

		kv.rf.Start(GeneralInput{
			OpType: LOAD_SHARD,
			Input: PullShardData{
				ShardInfo:    req.ShardInfo,
				Clients:      client,
				StateMachine: state,
			},
		})
	}
}

func (kv *ShardKV) infoCleanShard(si ShardInfo, servers []string) {
	req := CleanShardRequest{ShardInfo: si}
	for _, srv := range servers {
		var resp CleanShardResponse
		srvi := kv.make_end(srv)
		srvi.Call("ShardKV.CleanShard", &req, &resp)
		if resp.RPCInfo == SUCCEEDED_REQUEST || resp.RPCInfo == ALREADY_CLEAN {
			kv.rf.Start(GeneralInput{OpType: CLEAN_INFO_SHARD, Input:si})
			return
		}
	}
}

// tryLoadConfiguration 试图应用一个Config
// 应用新Config的成功与失败是否应该需要知道？不需要。
func (kv *ShardKV) tryLoadConfiguration(r *GeneralInput) (success bool) {
	config := r.Input.(shardctrler.Config)
	// 再次确认是否可以应用
	if config.Num <= kv.conf.Num || kv.step < 0 {
		// kv.Log("can't apply %+v", config)
		return
	}

	// 强保证就是必须清理全部遗留才可应用新Config

	success = true
	kv.Log("成功应用Config%d", config.Num)
	// 现在的config成为旧的config
	var oldConfig shardctrler.Config
	shardctrler.CopyConfig(&oldConfig, &kv.conf)

	// 对初始化的特殊判断
	if oldConfig.Num == 0 {
		shardctrler.CopyConfig(&kv.conf, &config)
		kv.oldConf = oldConfig
		return
	}

	for sIdx, ngid := range config.Shards {
		ogid := oldConfig.Shards[sIdx]
		switch {
		// 已经拥有的分片
		case ogid == kv.gid && ngid == kv.gid:

		// 需要取的分片
		case ogid != kv.gid && ngid == kv.gid:
			kv.ShardsToPull[sIdx] = true
			kv.step--

		// 需要给出的分片
		case ogid == kv.gid && ngid != kv.gid:
			kv.ShardsToDiscard[sIdx] = true
			// 将分片数据、和判断幂等的信息转移，然后清空当前分片数据和幂等信息
			state := kv.state[sIdx]
			kv.historyState[sIdx][oldConfig.Num] = state
			kv.state[sIdx] = make(map[string]string)
			clients := kv.clients[sIdx]
			kv.historyClients[sIdx][oldConfig.Num] = clients
			kv.clients[sIdx] = make(map[string]int64)
			kv.step--
		// 无关的分片
		default:
		}
	}

	// 更新新的config和旧的config
	shardctrler.CopyConfig(&kv.conf, &config)
	kv.oldConf = oldConfig

	// 至此，
	// step=需要拉取和删除的分片数
	// ShardsToPull记录哪些分片需要拉取
	// ShardsToDiscard记录哪些分片需要清除
	// 已经是新的Config
	// 非当前分片的数据已转移至历史区域
	// 紧接着需要立即持久化
	return
}

// tryLoadShard加载一个分片
// 加载一个分片的成功与否是否需要知道？不需要。
func (kv *ShardKV) tryLoadShard(r *GeneralInput) (success bool) {
	data := r.Input.(PullShardData)

	// 当且仅当该分片还需拉取
	if data.ConfigNum != kv.oldConf.Num || kv.step == 0 || !kv.ShardsToPull[data.Shard] {
		return
	}
	success = true
	kv.Log("成功加载分片 %+v", data.ShardInfo)
	kv.ShardsToPull[data.Shard] = false
	kv.ShardsToInfoDiscard[data.Shard] = true
	kv.state[data.Shard] = make(map[string]string)
	for k, v := range data.StateMachine {
		kv.state[data.Shard][k] = v
	}
	kv.clients[data.Shard] = make(map[string]int64)
	for k, v := range data.Clients {
		kv.clients[data.Shard][k] = v
	}
	return
}

func (kv *ShardKV) tryCleanInfoShard(idx, term int, r *GeneralInput) (success bool) {
	data := r.Input.(ShardInfo)

	// 当且仅当该分片还需删除
	if data.ConfigNum != kv.oldConf.Num || kv.step == 0 || !kv.ShardsToInfoDiscard[data.Shard] {
		return
	}
	success = true
	kv.Log("成功通知清理分片%+v", data)
	kv.ShardsToInfoDiscard[data.Shard] = false
	kv.step++
	return
}

func (kv *ShardKV) tryCleanShard(idx, term int, r *GeneralInput) (success bool) {
	data := r.Input.(ShardInfo)

	// 当且仅当该分片还需删除
	if data.ConfigNum != kv.oldConf.Num || kv.step == 0 || !kv.ShardsToDiscard[data.Shard] {
		if ch, ok := kv.signalChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.signalChans[idx], term)
		}
		return
	}
	success = true
	kv.Log("成功清理分片%+v", data)
	kv.ShardsToDiscard[data.Shard] = false
	kv.step++
	delete(kv.historyState[data.Shard], data.ConfigNum)
	delete(kv.historyClients[data.Shard], data.ConfigNum)
	if ch, ok := kv.signalChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCEEDED_REQUEST}
		delete(kv.signalChans[idx], term)
	}
	return
}
