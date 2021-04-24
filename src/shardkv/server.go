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
	clients        [shardctrler.NShards]map[string]int64  // sequence number for each known client
	state          [shardctrler.NShards]map[string]string // state machine

	conf    shardctrler.Config // latest config

	shardState [shardctrler.NShards]int

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	dead         int32

	signalChans map[int]map[int]chan GeneralOutput

}

func (kv *ShardKV) Get(args *GetRequest, reply *GetResponse) {
	// defer func() {
	// 	kv.Debug("Get RPC returns, %+v", *reply)
	// }()

	req := GeneralInput{
		OpType:  GET,
		Key:     args.Key,
		ClerkId: args.ClerkId,
	}

	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.RPCInfo = resp.RPCInfo
	reply.Value = resp.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	// defer func() {
	// 	kv.Debug("PutAppend RPC returns, %+v", *reply)
	// }()

	req := GeneralInput{
		OpType:  args.OpType,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
	}

	resp := kv.tryApplyAndGetResult(req)

	// for debug printing
	reply.Key = args.Key
	reply.ClerkId = args.ClerkId
	reply.OpType = args.OpType
	reply.RPCInfo = resp.RPCInfo
	reply.Value = resp.Value
}

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
	kv.signalChans = make(map[int]map[int]chan GeneralOutput)
	kv.Debug("****[KV STARTED]****")

	for i := 0; i < shardctrler.NShards; i++ {
		kv.clients[i] = make(map[string]int64)
		kv.state[i] = make(map[string]string)
	}

	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

	go kv.executeLoop()
	go kv.configListenLoop()
	for i := 0; i < shardctrler.NShards; i++ {
		go kv.shardOperationLoop(i)
	}

	return kv
}
