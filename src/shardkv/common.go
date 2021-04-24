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

// operation type
const (
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	LOAD_SHARD    = "加载分片"
	CLEAN_SHARD   = "清理分片"
	UPDATE_CONFIG = "更改配置"
)

// rpc information
const (
	SUCCESS           = "成功请求"
	FAILED_REQUEST    = "失败请求"
	DUPLICATE_REQUEST = "重复请求"
	WRONG_LEADER      = "错误领袖"
	WRONG_GROUP       = "错误集群"
	APPLY_TIMEOUT     = "请求超时"
)

// timeout settings
const (
	SHARD_OPERATION_INTERVAL = 100 * time.Millisecond
	CONFIG_LISTEN_INTERVAL   = 100 * time.Millisecond // 监听多集群配置变化的间隔时间
	CONFIG_LISTEN_TIMEOUT    = 1000 * time.Millisecond
	INTERNAL_TIMEOUT         = 500 * time.Millisecond // 内部逻辑处理最大允许时长，超时后RPC将提前返回
	LOCK_TIMEOUT             = 200 * time.Millisecond
)

// shard state
const (
	NOTINCHARGE  = iota // 不由该集群负责，且不在该集群中
	INCHARGE            // 由该集群负责，且在该集群中
	RECEIVING           // 由该集群负责，但目前还不在该集群中，正在接收
	TRANSFERRING        // 不由该集群负责，但目前还在该集群在，正在转移
)

type ClientInfo struct {
	Uid int64
	Seq int64
}

type GeneralInput struct {
	Key    string
	Value  string
	OpType string
	ClientInfo
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
	ClientInfo
}

type PutAppendResponse struct {
	Key        string // redundant info
	OpType     string // redundant info
	Value      string // redundant info
	ClientInfo        // redundant info
	RPCInfo    string
}

type GetRequest struct {
	Key string
	ClientInfo
}

type GetResponse struct {
	Key        string // redundant info
	Value      string
	RPCInfo    string
	ClientInfo // redundant info
}

type ReceiveShardRequest struct {
	SingleShardData
}

type ReceiveShardResponse struct {
	SingleShardInfo // redudant info
	RPCInfo         string
}

type SingleShardInfo struct {
	SenderGid int
	ConfigIdx int
	Shard     int
}

type SingleShardData struct {
	SingleShardInfo
	State   map[string]string
	Clients map[int64]int64
}

func (c ClientInfo) String() string {
	return fmt.Sprintf("[KV-CLI %d|SEQ-%d]", c.Uid, c.Seq)
}

func (r PutAppendRequest) String() string {
	return r.ClientInfo.String() + fmt.Sprintf("[%s K-%s V-%s]", r.OpType, r.Key, r.Value)
}

func (r PutAppendResponse) String() string {
	return r.ClientInfo.String() + fmt.Sprintf("[%s K-%s %s]", r.OpType, r.Key, r.RPCInfo)
}

func (r GetRequest) String() string {
	return r.ClientInfo.String() + fmt.Sprintf("[Get K-%s]", r.Key)
}

func (r GetResponse) String() string {
	return r.ClientInfo.String() + fmt.Sprintf("[Get K-%s %s]", r.Key, r.RPCInfo)
}

func (s SingleShardInfo) String() string {
	return fmt.Sprintf("[S:%d|C:%d|SENDER-GID:%d]", s.Shard, s.ConfigIdx, s.SenderGid)
}
