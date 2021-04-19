package shardctrler

import (
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	lockName string
	lockTime time.Time

	// Your data here.
	distros map[int]map[int]chan RaftResponse // distribution channels
	clients map[string]int64                   // sequence number for each known client
	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinRequest, reply *JoinResponse) {
	// Your code here.
	req := RaftRequest{
		OpType:  JOIN,
		ClerkId: args.ClerkId,
		Input:   args.Servers,
	}
	resp := sc.tryApplyAndGetResult(req)
	reply.RPCInfo = resp.RPCInfo
}

func (sc *ShardCtrler) Leave(args *LeaveRequest, reply *LeaveResponse) {
	// Your code here.
	req := RaftRequest{
		OpType:  LEAVE,
		ClerkId: args.ClerkId,
		Input:   args.GIDs,
	}
	resp := sc.tryApplyAndGetResult(req)
	reply.RPCInfo = resp.RPCInfo
}

func (sc *ShardCtrler) Move(args *MoveRequest, reply *MoveResponse) {
	// Your code here.
	req := RaftRequest{
		OpType:  MOVE,
		ClerkId: args.ClerkId,
		Input:   args.Movable,
	}
	resp := sc.tryApplyAndGetResult(req)
	reply.RPCInfo = resp.RPCInfo
}

func (sc *ShardCtrler) Query(args *QueryRequest, reply *QueryResponse) {
	// Your code here.
	req := RaftRequest{
		OpType:  QUERY,
		ClerkId: args.ClerkId,
		Input:   args.Num,
	}
	resp := sc.tryApplyAndGetResult(req)
	if resp.RPCInfo != SUCCESS {
		reply.RPCInfo = resp.RPCInfo
		return
	}
	src := resp.Output.(Config)
	shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		shards[i] = src.Shards[i]
	}
	groups := make(map[int][]string)
	num := src.Num

	for k, v := range src.Groups {
		arr := make([]string, len(v))
		copy(arr, v)
		groups[k] = arr
	}
	reply.Config = Config{
		Num:    num,
		Shards: shards,
		Groups: groups,
	}
	reply.RPCInfo = resp.RPCInfo
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clients = make(map[string]int64)
	sc.distros = make(map[int]map[int]chan RaftResponse)

	go sc.executeLoop()

	return sc
}
