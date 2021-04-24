package shardctrler

import (
	"sync/atomic"
	"time"

	"6.824/raft"
)

func (sc *ShardCtrler) lock(namespace string) {
	sc.mu.Lock()
	sc.lockname = namespace
	sc.locktime = time.Now()
}

func (sc *ShardCtrler) unlock() {
	if d := time.Since(sc.locktime); d >= LOCK_TIMEOUT {
		sc.error("UNLOCK[%s] too long, cost %+v", sc.lockname, d)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	Debug("========" + SRV_FORMAT + "CRASHED========", sc.me)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func CopyConfig(des, src *Config) {
	
	// copy num
	des.Idx = src.Idx
	
	// copy shards
	for i := range des.Shards {
		des.Shards[i] = src.Shards[i]
	}
	des.Groups = make(map[int][]string)

	// copy groups
	for k, v := range src.Groups {
		arr := make([]string, len(v))
		copy(arr, v)
		des.Groups[k] = arr
	}
}