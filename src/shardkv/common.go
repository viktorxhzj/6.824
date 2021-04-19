package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	NIL    = "Nil"

	RUNNING  = 0
	RECONFIG = 1

	SUCCESS           = "成功"
	NETWORK_FAILURE   = "超时"
	WRONG_LEADER      = "错误领袖"
	FAILED_REQUEST    = "失败重试"
	DUPLICATE_REQUEST = "幂等拦截"
	WRONG_GROUP       = "错误集群"

	NO_OP_INTERVAL         = 1000
	CONFIG_LISTEN_INTERVAL = 50
)

type ClerkId struct {
	Uid string
	Seq int64
}

type RaftRequest struct {
	Key    string
	Value  string
	OpType string
	ClerkId
}

type RaftResponse struct {
	Value   string
	RPCInfo string
}

// Put or Append
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
