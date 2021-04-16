package shardctrler

import (
	"sort"
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
	Debug(sc.me, "开启通道[%d|%d]%+v {%+v}", idx, term, ch, *req)
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
	Debug(sc.me, "关闭通道[%d|%d]%+v {%+v %+v}", idx, term, ch, req.ClerkId, req.OpType)
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
			config := sc.configs[len(sc.configs)-1]
			Debug(sc.me, "CONF %d [%+v]", config.Num, config.Shards)
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
			config := sc.configs[len(sc.configs)-1]
			Debug(sc.me, "CONF %d [%+v]", config.Num, config.Shards)
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
			config := sc.configs[len(sc.configs)-1]
			Debug(sc.me, "CONF %d [%+v]", config.Num, config.Shards)
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
			Debug(sc.me, "CONF %d [%+v]", config.Num, config.Shards)
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
		Debug(sc.me, "Distro %+v", sc.distro)
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
		Num: lastConfig.Num + 1,
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


func (sc *ShardCtrler) joinGroups(servers map[int][]string) {
	if len(sc.configs) == 0 {
		panic("No init config")
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num: lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	// if config # = 0, len(list) = 0
	var list MappingList
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
		list = append(list, ShardMapping{GID: g})
	}
	for g, srvs := range servers {
		if _, ok := newConfig.Groups[g]; ok {
			panic("Already exists")
		}
		newConfig.Groups[g] = srvs
	}
	for s := 0; s < NShards; s++ {
		g := lastConfig.Shards[s]
		for j := range list {
			if list[j].GID == g {
				list[j].Shards = append(list[j].Shards, s)
			}
		}
	}
	sort.Sort(list)

	for ng := range servers {
		sm := ShardMapping{GID: ng}
		if len(list) == 0 {
			for s := 0; s < NShards; s++ {
				sm.Shards = append(sm.Shards, s)
			}
		} else {
			lsm := list[len(list)-1]
			n := len(lsm.Shards)/2
			var j int
			for ;j < n; j++ {
				sm.Shards = append(sm.Shards, lsm.Shards[j])
			}
			lsm.Shards = lsm.Shards[j+1:]
		}
		list = append(list, sm)
		sort.Sort(list)
	}

	for _, sm := range list {
		for _, s := range sm.Shards {
			newConfig.Shards[s] = sm.GID
		}
	}
	
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leaveGroups(gids []int) {
	if len(sc.configs) == 0 {
		panic("No init config")
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num: lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	// if config # = 0, len(list) = 0
	var list MappingList
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
		list = append(list, ShardMapping{GID: g})
	}
	if len(list) == 0 {
		panic("config no group")
	}
	for s := 0; s < NShards; s++ {
		g := lastConfig.Shards[s]
		for j := range list {
			if list[j].GID == g {
				list[j].Shards = append(list[j].Shards, s)
			}
		}
	}
	sort.Sort(list)

	for _, lg := range gids {
		delete(newConfig.Groups, lg)
	}
	// lg = leaving group
	for i, lg := range gids {
		if list[0].GID == lg && len(list) == 1 {
			if i != len(gids) - 1 {
				panic("...")
			}
			list = []ShardMapping{}
			break
		}
		var lgsm ShardMapping
		var k int
		for j := range list {
			if list[j].GID == lg {
				lgsm = list[j]
				k = j
				break
			}
		}
		if list[0].GID == lg {
			list[1].Shards = append(list[1].Shards, lgsm.Shards...)
		} else {
			list[0].Shards = append(list[0].Shards, lgsm.Shards...)
		}
		list = append(list[:k], list[k+1:]...)
		sort.Sort(list)
	}

	for _, sm := range list {
		for _, s := range sm.Shards {
			newConfig.Shards[s] = sm.GID
		}
	}
	
	sc.configs = append(sc.configs, newConfig)
}

type ShardMapping struct {
	GID int
	Shards []int
}

type MappingList []ShardMapping

func (l MappingList) Len() int {
	return len(l)
}
func (l MappingList) Less(i, j int) bool {
	if len(l[i].Shards) != len(l[j].Shards) {
		return len(l[i].Shards) < len(l[j].Shards)
	}
	return l[i].GID < l[j].GID
}
func (l MappingList) Swap(i, j int) {
	k := l[i]
	l[i] = l[j]
	l[j] = k
}
