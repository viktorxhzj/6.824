package shardctrler

import (
	"container/heap"
	"time"
)

func (sc *ShardCtrler) tryApplyAndGetResult(req RaftRequest) (resp RaftResponse) {

	idx, term, ok := sc.rf.Start(req)
	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}
	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	sc.lock("try apply")
	ch := make(chan RaftResponse, 1)
	if mm := sc.distros[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		sc.distros[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	sc.unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	t := time.NewTimer(INTERNAL_MAX_DURATION)
	select {
	case resp = <-ch:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.removeDistro(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		return
	case <-t.C:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.removeDistro(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		resp.RPCInfo = INTERNAL_TIMEOUT
		return
	}
}

func (sc *ShardCtrler) removeDistro(idx, term int) {
	sc.lock("remove distro")
	delete(sc.distros[idx], term)
	sc.unlock()
}

func (sc *ShardCtrler) executeLoop() {

main:
	for !sc.killed() {

		msg := <-sc.applyCh

		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.lock("execute loop")

		// deal with snapshot
		if msg.SnapshotValid {
			panic("shard ctrler should not have snapshot")
		}
		// deal with commands
		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(RaftRequest)

		// 幂等性校验
		seq := sc.clients[r.Uid]
		if r.OpType != QUERY && r.Seq <= seq {
			if ch, ok := sc.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: DUPLICATE_REQUEST}
				delete(sc.distros[idx], term)
			}
			sc.unlock()
			continue main
		}

		sc.clients[r.Uid] = r.Seq

		switch r.OpType {
		case JOIN:
			servers := r.Input.(map[int][]string)
			sc.joinGroups(servers)
			if ch, ok := sc.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS}
				delete(sc.distros[idx], term)
			}

		case LEAVE:
			gids := r.Input.([]int)
			sc.leaveGroups(gids)
			if ch, ok := sc.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS}
				delete(sc.distros[idx], term)
			}

		case MOVE:
			movable := r.Input.(Movable)
			sc.moveOneShard(movable)
			if ch, ok := sc.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS}
				delete(sc.distros[idx], term)
			}

		case QUERY:
			num := r.Input.(int)
			var config Config
			if num != -1 && num < len(sc.configs) {
				config = sc.configs[num]
			} else {
				config = sc.configs[len(sc.configs)-1]
			}
			if ch, ok := sc.distros[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Output: config}
				delete(sc.distros[idx], term)
			}
		}
		sc.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
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
