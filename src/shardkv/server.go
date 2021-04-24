package shardkv

import (
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	// 互斥锁相关字段
	mu       sync.Mutex
	locktime time.Time
	lockname string

	// 集群相关信息
	me       int                 // 该节点在所在集群中的Id
	gid      int                 // 所在集群的全局Id
	scc      *shardctrler.Client // 配置集群的客户端
	make_end func(string) *labrpc.ClientEnd

	// 持久化信息
	clientSeq   [shardctrler.NShards]map[int64]int64   // sequence number for each known client
	shardedData [shardctrler.NShards]map[string]string // state machine

	conf shardctrler.Config // latest config

	shardState [shardctrler.NShards]int

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	dead         int32

	sigChans map[int]map[int]chan GeneralOutput
}

// Get returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (kv *ShardKV) Get(args *GetRequest, reply *GetResponse) {
	defer func() {
		kv.lock("Get RPC returns")
		kv.info("Get RPC returns, %+v", *reply)
		kv.unlock()
	}()

	in := GeneralInput{
		OpType:     GET,
		Key:        args.Key,
		ClientInfo: args.ClientInfo,
	}

	out := kv.tryApplyAndGetResult(in)
	reply.Key = args.Key
	reply.ClientInfo = args.ClientInfo
	reply.RPCInfo = out.RPCInfo
	reply.Value = out.Value
}

// PutAppend returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (kv *ShardKV) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	defer func() {
		kv.lock("PutAppend RPC returns")
		kv.info("PutAppend RPC returns, %+v", *reply)
		kv.unlock()
	}()

	in := GeneralInput{
		OpType:     args.OpType,
		Key:        args.Key,
		Value:      args.Value,
		ClientInfo: args.ClientInfo,
	}

	out := kv.tryApplyAndGetResult(in)
	reply.Key = args.Key
	reply.ClientInfo = args.ClientInfo
	reply.OpType = args.OpType
	reply.RPCInfo = out.RPCInfo
	reply.Value = out.Value
}

// ReceiveShard returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (kv *ShardKV) ReceiveShard(args *ReceiveShardRequest, reply *ReceiveShardResponse) {
	defer func() {
		kv.lock("ReceiveShard RPC returns")
		kv.info("ReceiveShard RPC returns, %+v", *reply)
		kv.unlock()
	}()

	in := GeneralInput{
		OpType: LOAD_SHARD,
		Input:  args.SingleShardData,
	}
	out := kv.tryApplyAndGetResult(in)
	reply.SingleShardInfo = args.SingleShardInfo
	reply.RPCInfo = out.RPCInfo
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.scc = shardctrler.MakeClient(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sigChans = make(map[int]map[int]chan GeneralOutput)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.clientSeq[i] = make(map[int64]int64)
		kv.shardedData[i] = make(map[string]string)
	}

	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

	go kv.executeLoop()
	go kv.configListenLoop()
	for i := 0; i < shardctrler.NShards; i++ {
		go kv.shardOperationLoop(i)
	}

	Debug("========"+SRV_FORMAT+"STARTED========", kv.gid, kv.me)
	return kv
}
