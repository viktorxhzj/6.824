package shardctrler

import (
	"container/heap"
	"time"
)

func (sc *ShardCtrler) tryApplyAndGetResult(req *RaftRequest, resp *RaftResponse) {

	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	sc.mu.Lock()
	var idx, term int
	var ok bool
	switch req.OpType {
	case NIL:
		idx, term, ok = sc.rf.Start(nil)
	default:
		idx, term, ok = sc.rf.Start(*req)
	}

	if !ok {
		sc.mu.Unlock()
		resp.RPCInfo = WRONG_LEADER
		return
	}

	ch := make(chan RaftResponse)
	// Debug(sc.me, "开启通道[%d|%d]%+v {%+v}", idx, term, ch, *req)
	if mm := sc.distro[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		sc.distro[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	sc.mu.Unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	rr := <-ch
	close(ch)
	resp.Output = rr.Output
	resp.RPCInfo = rr.RPCInfo
	// Debug(sc.me, "关闭通道[%d|%d]%+v {%+v %+v}", idx, term, ch, req.ClerkId, req.OpType)
}

func (sc *ShardCtrler) executeLoop() {

main:
	for !sc.killed() {

		msg := <-sc.applyCh

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.mu.Lock()

		// deal with snapshot
		if msg.SnapshotValid {
			panic("shard ctrler should not have snapshot")
		}
		// deal with commands
		idx, term := msg.CommandIndex, msg.CommandTerm
		r, ok := msg.Command.(RaftRequest)
		Debug(sc.me, "收到日志[%d|%d] {%+v}", idx, term, msg.Command)
		// No-op
		if !ok {
			mm := sc.distro[idx]
			for _, v := range mm {
				v <- RaftResponse{RPCInfo: FAILED_REQUEST}
			}
			delete(sc.distro, idx)
			sc.mu.Unlock()
			continue main
		}
		seq := sc.clerks[r.Uid]

		// 幂等性校验
		if r.OpType != QUERY && r.Seq <= seq {
			mm := sc.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: DUPLICATE_REQUEST}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}
			delete(sc.distro, idx)
			sc.mu.Unlock()
			continue main
		}

		sc.clerks[r.Uid] = r.Seq

		switch r.OpType {
		case JOIN:
			servers := r.Input.(map[int][]string)
			sc.joinGroups(servers)
			mm := sc.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}

		case LEAVE:
			gids := r.Input.([]int)
			sc.leaveGroups(gids)
			mm := sc.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}

		case MOVE:
			movable := r.Input.(Movable)
			sc.moveOneShard(movable)
			mm := sc.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}

		case QUERY:
			num := r.Input.(int)
			var config Config
			if num != -1 && num < len(sc.configs) {
				config = sc.configs[num]
			} else {
				config = sc.configs[len(sc.configs)-1]
			}
			Debug(sc.me, "%+v", config)
			mm := sc.distro[idx]
			for k, v := range mm {
				if k == term {
					v <- RaftResponse{RPCInfo: SUCCESS, Output: config}
				} else {
					v <- RaftResponse{RPCInfo: FAILED_REQUEST}
				}
			}
		}
		delete(sc.distro, idx)
		// Debug(sc.me, "Distro %+v", sc.distro)
		sc.mu.Unlock()

		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (sc *ShardCtrler) noopLoop() {

	for !sc.killed() {
		time.Sleep(NO_OP_INTERVAL * time.Millisecond)
		req := RaftRequest{OpType: NIL}
		resp := RaftResponse{}
		Debug(sc.me, "Periodically No-Op")
		go sc.tryApplyAndGetResult(&req, &resp)
	}
}

// [0,9]
// [0,4] [5,9]
// [0,1] [2,4] [5,9]
// [0,1] [2,4] [5,6] [7,9]
// [0,1] [2]   [3,4] [5,6] [7,9]
// [0,1] [2]   [3,4] [5,6] [7]   [8,9]
// [0]   [1]   [2]   [3,4] [5,6] [7]   [8,9]

func (sc *ShardCtrler) moveOneShard(m Movable) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
	}
	for s := range lastConfig.Shards {
		newConfig.Shards[s] = lastConfig.Shards[s]
	}
	newConfig.Shards[m.Shard] = m.GID
	sc.configs = append(sc.configs, newConfig)
}

// 5 5
// 3 3 4
// 2 2 2 4
// 2 2 2 2 2
func (sc *ShardCtrler) joinGroups(servers map[int][]string) {
	if len(sc.configs) == 0 {
		panic("No init config")
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	gids := Heap{}
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
		heap.Push(&gids, g)
	}
	for g, srvs := range servers {
		newConfig.Groups[g] = srvs
		heap.Push(&gids, g)
	}

	reallocSlots(&newConfig, &gids)
	Debug(sc.me, "%+v", newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leaveGroups(lgids []int) {
	if len(sc.configs) == 0 {
		panic("No init config")
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
	}
	for _, lg := range lgids {
		delete(newConfig.Groups, lg)
	}
	gids := Heap{}
	for g := range newConfig.Groups {
		heap.Push(&gids, g)
	}

	reallocSlots(&newConfig, &gids)
	Debug(sc.me, "%+v", newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func reallocSlots(config *Config, gids *Heap) {
	size := gids.Len()
	if size == 0 {
		return
	}
	each, res := NShards/size, NShards%size
	var offset int
	for gids.Len() > 0 {
		g := heap.Pop(gids).(int)
		for i := 0; i < each; i++ {
			config.Shards[offset] = g
			offset++
		}
		if res > 0 {
			res--
			config.Shards[offset] = g
			offset++
		}
	}
}
