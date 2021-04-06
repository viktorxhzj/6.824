package kvraft

import "fmt"

type (
	RPCInfo string
	OpType  string
)

const (
	GET    OpType = "Get"
	PUT    OpType = "Put"
	APPEND OpType = "Append"
	NIL    OpType = "NIL"

	SUCCESS           RPCInfo = "成功"
	NETWORK_FAILURE   RPCInfo = "超时"
	WRONG_LEADER      RPCInfo = "非法领袖"
	FAILED_REQUEST    RPCInfo = "失败重试"
	DUPLICATE_REQUEST RPCInfo = "幂等拦截"

	NO_OP_INTERVAL          = 1000
	SIZE_DETECTION_INTERVAL = 2000
)

type ClerkId struct {
	Uid int64
	Seq int64
}

func (c ClerkId) String() string {
	return fmt.Sprintf("[CLI-%d SEQ-%d]", c.Uid, c.Seq)
}

type PutAppendRequest struct {
	Key   string
	Value string
	OpType
	ClerkId
}

type PutAppendResponse struct {
	RPCInfo
}

type GetRequest struct {
	Key string
	ClerkId
}

type GetResponse struct {
	RPCInfo
	Value string
}

type RaftRequest struct {
	Key   string
	Value string
	OpType
	ClerkId
}

func (r RaftRequest) String() string {
	return fmt.Sprintf("%+v [%+v K:%s V:%s]", r.ClerkId, r.OpType, r.Key, r.Value)
}

type RaftResponse struct {
	Key   string
	Value string
	OpType
	RPCInfo
	ClerkId
}

func (r RaftResponse) String() string {
	return fmt.Sprintf("%+v [%+v K:%s V:%s] %+v", r.ClerkId, r.OpType, r.Key, r.Value, r.RPCInfo)
}