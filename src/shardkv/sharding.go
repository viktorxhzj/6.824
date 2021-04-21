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

		t := time.NewTimer(2 * time.Second)
		ch := make(chan shardctrler.Config)
		var config shardctrler.Config
		go func(ch chan shardctrler.Config) { ch <- kv.scc.Query(-1) }(ch)
		select {
		case <-t.C:
			// fmt.Println("整活")
			continue outer
		case config = <-ch:
			kv.Debug("Normal Config Listen")
		}
		kv.lock("config listen")
		kv.Log("Old Config %d %+v", kv.oldConf.Num, kv.oldConf.Shards)
		kv.Log("Pulled Config %d %+v", config.Num, config.Shards)
		// 什么时候可以应用新的Config？
		// 1. 新的Config更新
		// 2. 当前不在reconfig
		if config.Num > kv.conf.Num && kv.step == 0 {
			kv.Log("try apply Config %d", config.Num)
			// RPC的结果的需要Deep-Copy
			cp := shardctrler.Config{}
			shardctrler.CopyConfig(&cp, &config)
			kv.rf.Start(GeneralInput{
				OpType: UPDATE_CONFIG,
				Input:  cp,
			})
		}
		kv.unlock()
	}
}

// tryLoadConfiguration 试图应用一个Config
func (kv *ShardKV) tryLoadConfiguration(r *GeneralInput) (success bool) {
	config := r.Input.(shardctrler.Config)
	// 再次确认是否可以应用
	if config.Num <= kv.conf.Num || kv.step < 0 {
		kv.Log("can't apply %+v", config)
		return
	}

	// 此时 kv.ShardIdsToPull[i]是必然Pull完毕的，加上自己已经拥有的
	// 可以确保该节点成功启用
	// 但是 kv.ShardIdsToDiscard[i]有可能还有遗留的Shard没有清理
	// 强保证和弱保证？
	// 强保证就是必须清理全部遗留才可应用新Config
	// 弱保证就是先应用新Config，然后后台协程清理。比如当前29，应用到30，则可开始清理<=28的。
	// 弱保证至多保留一层数据，可以采用
	// 采用弱保证方案的话step仅仅和pull挂钩

	success = true
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
			kv.ShardIdsToPull[sIdx] = true
			kv.step--

		// 需要给出的分片
		case ogid == kv.gid && ngid != kv.gid:
			kv.ShardIdsToDiscard[sIdx] = true
			// 将分片数据、和判断幂等的信息转移，然后清空当前分片数据和幂等信息
			state := kv.stateMachine[sIdx]
			kv.historyState[sIdx][oldConfig.Num] = state
			kv.stateMachine[sIdx] = make(map[string]string)
			clients := kv.clients[sIdx]
			kv.historyClients[sIdx][oldConfig.Num] = clients
			kv.clients[sIdx] = make(map[string]int64)
		// 无关的分片
		default:
		}
	}

	// 更新新的config和旧的config
	shardctrler.CopyConfig(&kv.conf, &config)
	kv.oldConf = oldConfig

	// 至此，
	// step=需要拉取的分片数
	// ShardIdsToPull记录哪些分片需要拉取
	// 已经是新的Config
	// 非当前分片的数据已转移至历史区域
	// 紧接着需要立即持久化
	kv.Log("成功应用新Config%d", kv.conf.Num)
	return
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
		for i := range kv.ShardIdsToPull {
			if kv.ShardIdsToPull[i] {
				kv.Log("Pull Shard %d", i)
				go kv.pullShard(i)
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
		if !leader || kv.oldConf.Num > req.ConfigNum || kv.step == 0 || !kv.ShardIdsToPull[sIdx] {
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
			OpType: PULL_SHARD,
			Input: PullShardData{
				ShardInfo:    req.ShardInfo,
				Clients:      client,
				StateMachine: state,
			},
		})
	}
}

func (kv *ShardKV) tryLoadShard(r *GeneralInput) (success bool) {
	data := r.Input.(PullShardData)

	if data.ConfigNum != kv.oldConf.Num || kv.step == 0 || !kv.ShardIdsToPull[data.Shard] {
		return
	}
	success = true
	kv.ShardIdsToPull[data.Shard] = false
	kv.step++
	kv.stateMachine[data.Shard] = make(map[string]string)
	for k, v := range data.StateMachine {
		kv.stateMachine[data.Shard][k] = v
	}
	kv.clients[data.Shard] = make(map[string]int64)
	for k, v := range data.Clients {
		kv.clients[data.Shard][k] = v
	}
	return
}

func (kv *ShardKV) infoCleanShardLoop() {
	for !kv.killed() {
		time.Sleep(INFO_CLEAN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}
		kv.lock("clean shard")
		// 如何判断已经拉取的分片？
		if kv.oldConf.Num == 0 {
			kv.unlock()
			continue
		}
		for sIdx, ngid := range kv.conf.Shards {
			ogid := kv.oldConf.Shards[sIdx]
			if ogid != kv.gid && ngid == kv.gid && !kv.ShardIdsToPull[sIdx] {
				gid := kv.oldConf.Shards[sIdx]
				servers := kv.oldConf.Groups[gid]
				shardInfo := ShardInfo{
					ConfigNum: kv.oldConf.Num,
					Shard:     sIdx,
				}
				kv.Log("info clean shard %+v", shardInfo)
				go kv.infoCleanShard(shardInfo, servers)
			}
		}
		kv.unlock()
	}
}

func (kv *ShardKV) infoCleanShard(si ShardInfo, servers []string) {
	req := CleanShardRequest{ShardInfo: si}
	for _, srv := range servers {
		var resp CleanShardResponse
		srvi := kv.make_end(srv)
		srvi.Call("ShardKV.CleanShard", &req, &resp)
		if resp.RPCInfo == SUCCEEDED_REQUEST || resp.RPCInfo == ALREADY_CLEAN {
			return
		}
	}
}

func (kv *ShardKV) cleanShardLoop() {
	// for !kv.killed() {
	// 	time.Sleep(CLEAN_SHARD_INTERVAL)
	// 	if _, leader := kv.rf.GetState(); !leader {
	// 		continue
	// 	}
	// 	kv.lock("clean shard loop")
	// 	for i := kv.oldConf.Num - 1; i > 0; i-- {
	// 		var count int
	// 		for j := 0; j < shardctrler.NShards; j++ {
	// 			// 有就去清除
	// 			if _, ok := kv.historyState[j][i]; ok {
	// 				kv.rf.Start(GeneralInput{
	// 					OpType: ACTIVE_CLEAN_SHARD,
	// 					Input: ShardInfo{
	// 						ConfigNum: i,
	// 						Shard:     j,
	// 					},
	// 				})
	// 				count++
	// 			}
	// 		}
	// 		if count == 0 {
	// 			break
	// 		}
	// 	}
	// 	kv.unlock()

	// }
}

func (kv *ShardKV) tryCleanShard(idx, term int, r *GeneralInput) (success bool) {
	data := r.Input.(ShardInfo)

	// 清除分片有两种可能
	// 一种是oldConf所对应的ToDiscard分片
	// 另一种是更早期的分片历史
	if data.ConfigNum > kv.oldConf.Num {
		if r.OpType == PASSIVE_CLEAN_SHARD {
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
				delete(kv.distros[idx], term)
			}
		}
		return
	}
	if data.ConfigNum == kv.oldConf.Num && !kv.ShardIdsToDiscard[data.Shard] {
		if r.OpType == PASSIVE_CLEAN_SHARD {
			if ch, ok := kv.distros[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: ALREADY_CLEAN}
				delete(kv.distros[idx], term)
			}
		}
		return
	}

	kv.Log("删除分片 %+v", data)
	success = true
	if data.ConfigNum == kv.oldConf.Num {
		kv.ShardIdsToDiscard[data.Shard] = false
	}
	delete(kv.historyState[data.Shard], data.ConfigNum)
	delete(kv.historyClients[data.Shard], data.ConfigNum)
	if r.OpType == PASSIVE_CLEAN_SHARD {
		if ch, ok := kv.distros[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: SUCCEEDED_REQUEST}
			delete(kv.distros[idx], term)
		}
	}
	return
}
