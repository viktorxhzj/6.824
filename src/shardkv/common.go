package shardkv

import (
	"fmt"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	GET              = "Get"
	PUT              = "Put"
	APPEND           = "Append"
	LOAD_SHARD       = "加载分片"
	CLEAN_SHARD      = "清理分片"
	CLEAN_INFO_SHARD = "清理通知分片"
	UPDATE_CONFIG    = "更改配置"

	SUCCEEDED_REQUEST = "成功请求"
	FAILED_REQUEST    = "失败请求"
	DUPLICATE_REQUEST = "重复请求"
	NETWORK_ERROR     = "网络波动"
	WRONG_LEADER      = "错误领袖"
	WRONG_GROUP       = "错误集群"
	ALREADY_CLEAN     = "已经删除"
	APPLY_TIMEOUT     = "请求超时"

	SHARD_OPERATION_INTERVAL = 100 * time.Millisecond
	CONFIG_LISTEN_INTERVAL   = 100 * time.Millisecond // 监听多集群配置变化的间隔时间
	CONFIG_LISTEN_TIMEOUT    = 1000 * time.Millisecond
	SHARD_PULL_INTERVAL      = 100 * time.Millisecond
	INFO_CLEAN_INTERVAL      = 1000 * time.Millisecond
	CLEAN_SHARD_INTERVAL     = 1000 * time.Millisecond
	INTERNAL_TIMEOUT         = 500 * time.Millisecond // 内部逻辑处理最大允许时长，超时后RPC将提前返回
	LOCK_TIMEOUT             = 1000 * time.Millisecond
)

// 一个分片尽可能涉及以下几种状态：
// 不在该集群中
// 不在该集群中，由该集群负责
// 在该集群中，由该集群负责
// 在该集群中，不由该集群负责，转移中
const (
	NOTINCHARGE  = 0
	RECEIVING    = -2
	INCHARGE     = 1
	TRANSFERRING = 2
)

type ClerkId struct {
	Uid string
	Seq int64
}

type GeneralInput struct {
	Key    string
	Value  string
	OpType string
	ClerkId
	Input interface{}
}

type GeneralOutput struct {
	Value   string
	Output  interface{}
	RPCInfo string
}

type PutAppendRequest struct {
	Key    string
	Value  string
	OpType string
	ClerkId
}

type PutAppendResponse struct {
	Key     string // redundant info
	OpType  string // redundant info
	Value   string
	ClerkId // redundant info
	RPCInfo string
}

type GetRequest struct {
	Key string
	ClerkId
}

type GetResponse struct {
	Key     string // redundant info
	Value   string
	RPCInfo string
	ClerkId // redundant info
}

type ShardInfo struct {
	Gid       int
	ConfigNum int
	Shard     int
}

type TargetInfo struct {
	ConfigNum int
	Servers   []string
}

type ReceiveShardRequest struct {
	ShardData
}

type ReceiveShardResponse struct {
	RPCInfo string
}

type ShardData struct {
	ShardInfo
	State   map[string]string
	Clients map[string]int64
}

type CleanShardRequest struct {
	ShardInfo
}

type CleanShardResponse struct {
	ShardInfo
	RPCInfo string
}

func (c ClerkId) String() string {
	return fmt.Sprintf("[%s|SEQ-%d]", c.Uid, c.Seq)
}

func (r PutAppendRequest) String() string {
	return r.ClerkId.String() + fmt.Sprintf("[%s K-%s V-%s]", r.OpType, r.Key, r.Value)
}

func (r PutAppendResponse) String() string {
	return r.ClerkId.String() + fmt.Sprintf("[%s K-%s %s]", r.OpType, r.Key, r.RPCInfo)
}

func (r GetRequest) String() string {
	return r.ClerkId.String() + fmt.Sprintf("[Get K-%s]", r.Key)
}

func (r GetResponse) String() string {
	return r.ClerkId.String() + fmt.Sprintf("[Get K-%s %s]", r.Key, r.RPCInfo)
}

func (s ShardInfo) String() string {
	return fmt.Sprintf("[S:%d|C:%d|GID:%d]", s.Shard, s.ConfigNum, s.Gid)
}
