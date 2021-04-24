package shardctrler

import (
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32
	lockname string
	locktime time.Time
	sigChans  map[int]map[int]chan GeneralOutput
	clientSeq  map[int64]int64
	configs  []Config
}

// Join returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (sc *ShardCtrler) Join(args *JoinRequest, reply *JoinResponse) {
	in := GeneralInput{
		OpType:     JOIN,
		ClientInfo: args.ClientInfo,
		Input:      args.Servers,
	}
	out := sc.tryApplyAndGetResult(in)
	reply.RPCInfo = out.RPCInfo
}

// Leave returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (sc *ShardCtrler) Leave(args *LeaveRequest, reply *LeaveResponse) {
	in := GeneralInput{
		OpType:     LEAVE,
		ClientInfo: args.ClientInfo,
		Input:      args.GIDs,
	}
	out := sc.tryApplyAndGetResult(in)
	reply.RPCInfo = out.RPCInfo
}

// Move returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (sc *ShardCtrler) Move(args *MoveRequest, reply *MoveResponse) {
	in := GeneralInput{
		OpType:     MOVE,
		ClientInfo: args.ClientInfo,
		Input:      args.Movement,
	}
	out := sc.tryApplyAndGetResult(in)
	reply.RPCInfo = out.RPCInfo
}

// Move returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS
func (sc *ShardCtrler) Query(args *QueryRequest, reply *QueryResponse) {
	in := GeneralInput{
		OpType:     QUERY,
		ClientInfo: args.ClientInfo,
		Input:      args.Idx,
	}
	out := sc.tryApplyAndGetResult(in)
	if out.RPCInfo != SUCCESS {
		reply.RPCInfo = out.RPCInfo
		return
	}
	src := out.Output.(Config)
	CopyConfig(&reply.Config, &src)
	reply.RPCInfo = out.RPCInfo
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clientSeq = make(map[int64]int64)
	sc.sigChans = make(map[int]map[int]chan GeneralOutput)

	go sc.executeLoop()
	Debug("========" + SRV_FORMAT + "STARTED========", sc.me)
	return sc
}
