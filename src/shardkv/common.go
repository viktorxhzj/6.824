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
	GET                 = "Get"
	PUT                 = "Put"
	APPEND              = "Append"
	PULL_SHARD          = "拉取分片"
	ACTIVE_CLEAN_SHARD  = "主动清理"
	PASSIVE_CLEAN_SHARD = "被动清理"
	UPDATE_CONFIG       = "更改配置"

	SUCCEEDED_REQUEST = "成功请求"
	FAILED_REQUEST    = "失败请求"
	DUPLICATE_REQUEST = "重复请求"
	NETWORK_ERROR     = "网络波动"
	INTERNAL_TIMEOUT  = "内部超时"
	WRONG_LEADER      = "错误领袖"
	WRONG_GROUP       = "错误集群"
	ALREADY_CLEAN     = "已经删除"

	CONFIG_LISTEN_INTERVAL = 50 * time.Millisecond // 监听多集群配置变化的间隔时间
	SHARD_PULL_INTERVAL    = 100 * time.Millisecond
	INFO_CLEAN_INTERVAL    = 1000 * time.Millisecond
	CLEAN_SHARD_INTERVAL   = 1000 * time.Millisecond
	INTERNAL_MAX_DURATION  = 500 * time.Millisecond // 内部逻辑处理最大允许时长，超时后RPC将提前返回
	LOCK_TIMEOUT = 2000 * time.Millisecond
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
	ConfigNum int
	Shard     int
}

type PullShardRequest struct {
	ShardInfo
}

type PullShardResponse struct {
	StateMachine map[string]string
	Clients      map[string]int64
	RPCInfo      string
}

type PullShardData struct {
	ShardInfo
	StateMachine map[string]string
	Clients      map[string]int64
}

type CleanShardRequest struct {
	ShardInfo
}

type CleanShardResponse struct {
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
	return fmt.Sprintf("[%d|%d]", s.Shard, s.ConfigNum)
}