package shardctrler

import (
	"container/heap"
	"time"

	"6.824/labgob"
	"6.824/raft"
)

func (sc *ShardCtrler) tryApplyAndGetResult(req GeneralInput) (resp GeneralOutput) {

	idx, term, ok := sc.rf.Start(req)
	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}
	/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
	sc.lock("try apply")
	ch := make(chan GeneralOutput, 1)
	if mm := sc.sigChans[idx]; mm == nil {
		mm = make(map[int]chan GeneralOutput)
		sc.sigChans[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	sc.unlock()
	/*--------------------CRITICAL SECTION--------------------*/

	t := time.NewTimer(INTERNAL_TIMEOUT)
	select {
	case resp = <-ch:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.removeSigChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		return
	case <-t.C:
		/*++++++++++++++++++++CRITICAL SECTION++++++++++++++++++++*/
		sc.removeSigChan(idx, term)
		/*--------------------CRITICAL SECTION--------------------*/
		resp.RPCInfo = APPLY_TIMEOUT
		return
	}
}

func (sc *ShardCtrler) removeSigChan(idx, term int) {
	sc.lock("remove sigChan")
	delete(sc.sigChans[idx], term)
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
			sc.error("shard ctrler should not have snapshot")
			sc.unlock()
			continue main
		}
		// deal with commands
		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(GeneralInput)
		sc.info("收到日志[%d|%d] %s", idx, term, r.OpType)

		// 幂等性校验
		seq := sc.clientSeq[r.Uid]
		if r.OpType != QUERY && r.Seq <= seq {
			if ch, ok := sc.sigChans[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
				delete(sc.sigChans[idx], term)
			}
			sc.unlock()
			continue main
		}

		sc.clientSeq[r.Uid] = r.Seq

		switch r.OpType {
		case JOIN:
			servers := r.Input.(map[int][]string)
			sc.joinGroups(servers)
			if ch, ok := sc.sigChans[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: SUCCESS}
				delete(sc.sigChans[idx], term)
			}

		case LEAVE:
			gids := r.Input.([]int)
			sc.leaveGroups(gids)
			if ch, ok := sc.sigChans[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: SUCCESS}
				delete(sc.sigChans[idx], term)
			}

		case MOVE:
			movable := r.Input.(Movement)
			sc.moveOneShard(movable)
			if ch, ok := sc.sigChans[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: SUCCESS}
				delete(sc.sigChans[idx], term)
			}

		case QUERY:
			num := r.Input.(int)
			var config Config
			if num != -1 && num < len(sc.configs) {
				config = sc.configs[num]
			} else {
				config = sc.configs[len(sc.configs)-1]
			}
			if ch, ok := sc.sigChans[idx][term]; ok {
				ch <- GeneralOutput{RPCInfo: SUCCESS, Output: config}
				delete(sc.sigChans[idx], term)
			}
		}
		sc.unlock()
		/*--------------------CRITICAL SECTION--------------------*/
	}
}

func (sc *ShardCtrler) moveOneShard(m Movement) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Idx:    lastConfig.Idx + 1,
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
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Idx:    lastConfig.Idx + 1,
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
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Idx:    lastConfig.Idx + 1,
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

func init() {
	labgob.Register(JoinRequest{})
	labgob.Register(JoinResponse{})

	labgob.Register(LeaveRequest{})
	labgob.Register(LeaveResponse{})

	labgob.Register(MoveRequest{})
	labgob.Register(MoveResponse{})

	labgob.Register(QueryRequest{})
	labgob.Register(QueryResponse{})

	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})

	labgob.Register(raft.RequestVoteRequest{})
	labgob.Register(raft.RequestVoteResponse{})

	labgob.Register(GeneralInput{})
	labgob.Register(GeneralOutput{})

	labgob.Register(map[int][]string{})
	labgob.Register(Movement{})
}
