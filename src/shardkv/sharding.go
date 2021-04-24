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
		kv.lock("")
		c := kv.conf.Num
		kv.unlock()
		ch := make(chan shardctrler.Config)
		var config shardctrler.Config
		go func(configNum int) { ch <- kv.scc.Query(configNum) }(c+1)
		select {
		case <-t.C:
			kv.Debug("监听配置超时")
			continue outer
		case config = <-ch:
		}

		// 并不是真正的可以canApply
		var canApply bool
		kv.lock("监听配置")
		canApply = config.Num == kv.conf.Num + 1
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

// tryLoadConfiguration 试图应用一个Config
// 应用新Config的成功与失败是否应该需要知道？不需要。
func (kv *ShardKV) tryLoadConfiguration(r *GeneralInput) (success bool) {
	config := r.Input.(shardctrler.Config)
	if config.Num != kv.conf.Num + 1{
		return
	}
	for s := range kv.shardState {
		if kv.shardState[s] != INCHARGE && kv.shardState[s] != NOTINCHARGE {
			return
		}
	}

	success = true
	// 现在的config成为旧的config
	var oldConfig shardctrler.Config
	shardctrler.CopyConfig(&oldConfig, &kv.conf)

	// 对初始化的特殊判断
	if oldConfig.Num == 0 {
		for s, ngid := range config.Shards {
			if ngid == kv.gid {
				kv.shardState[s] = INCHARGE
			}
		}
		shardctrler.CopyConfig(&kv.conf, &config)
		kv.Log("成功应用Config%d", config.Num)
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
			panic("wrong state")
		}
	}

	// 更新新的config
	shardctrler.CopyConfig(&kv.conf, &config)
	kv.Log("成功应用Config%d", config.Num)
	return
}
