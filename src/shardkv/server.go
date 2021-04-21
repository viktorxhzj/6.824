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
	me       int                // 该节点在所在集群中的Id
	gid      int                // 所在集群的全局Id
	scc      *shardctrler.Clerk // 配置集群的客户端
	make_end func(string) *labrpc.ClientEnd
	// ctrlers      []*labrpc.ClientEnd


	// 持久化信息
	clients      [shardctrler.NShards]map[string]int64                       // sequence number for each known client
	stateMachine [shardctrler.NShards]map[string]string // state machine
	historyState map[int]map[int]map[string]string // configNum -> shard -> data
	historyClients map[int]map[int]map[string]int64

	conf     shardctrler.Config // latest config
	prevConf shardctrler.Config // previous config
	step int // 负数代表正在reconfiguration, 绝对值等于还缺少的分片数量

	// 当采用新的配置时，记录需要拉取的分片，已经拉取的分片和需要丢弃的分片
	// 轮询拉取需要拉取的分片i，拉取成功并成功应用时，该分片被置为已拉取
	// 轮询通知需要丢弃已拉取的分片，接受成功并成功应用时，丢弃该分片并置为已丢弃
	ShardIdsToPull [shardctrler.NShards]bool
	ShardIdsToDiscard [shardctrler.NShards]bool


	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	dead         int32

	distros      map[int]map[int]chan GeneralOutput      // distribution channels

}

func (kv *ShardKV) Get(args *GetRequest, reply *GetResponse) {
	defer func() {
		kv.Debug("Get RPC returns, %+v", *reply)
	}()

	req := GeneralInput{
		OpType: GET,
		Input: *args,
	}

	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.RPCInfo = resp.RPCInfo
	reply.Value = resp.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	defer func() {
		kv.Debug("PutAppend RPC returns, %+v", *reply)
	}()

	req := GeneralInput{
		OpType: args.OpType,
		Input: *args,
	}

	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.OpType = args.OpType
	reply.RPCInfo = resp.RPCInfo
	reply.Value = resp.Value
}

func (kv *ShardKV) PullShard(args *PutAppendRequest, reply *PutAppendResponse) {}

func (kv *ShardKV) RemoveShard(args *PutAppendRequest, reply *PutAppendResponse) {}



//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.scc = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.distros = make(map[int]map[int]chan GeneralOutput)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.clients[i] = make(map[string]int64)
		kv.stateMachine[i] = make(map[string]string)
	}

	kv.historyClients = make(map[int]map[int]map[string]int64)
	kv.historyState = make(map[int]map[int]map[string]string)


	go kv.executeLoop()
	go kv.configListenLoop()
	go kv.pullShardLoop()

	return kv
}
