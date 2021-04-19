package kvraft

import (
	"fmt"
	"time"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	NIL    = "Nil"

	SUCCESS           = "成功"
	NETWORK_FAILURE   = "网络超时"
	SERVER_TIMEOUT    = "内部超时"
	WRONG_LEADER      = "非法领袖"
	FAILED_REQUEST    = "失败重试"
	DUPLICATE_REQUEST = "幂等拦截"
)

const CLIENT_REQUEST_INTERVAL = 100 * time.Millisecond
const APPLY_TIMEOUT = 500 * time.Millisecond

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

type PutAppendRequest struct {
	Key    string
	Value  string
	OpType string
	ClerkId
}

type PutAppendResponse struct {
	Key     string // redundant info
	OpType  string // redundant info
	Value   string // value immediately after execution
	ClerkId        // redundant info
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
