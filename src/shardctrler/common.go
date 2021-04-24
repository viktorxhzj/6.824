package shardctrler

import (
	"fmt"
	"time"
)

const (
	NShards = 10 // number of shards
	INTERNAL_TIMEOUT = 500 * time.Millisecond // allowed duration for processing request
	LOCK_TIMEOUT     = 200 * time.Millisecond // allowed duration inside a lock
)

// operation type
const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
	NIL   = "NIL"
)

// rpc information
const (
	SUCCESS           = "成功请求"
	APPLY_TIMEOUT     = "请求超时"
	WRONG_LEADER      = "错误领袖"
	FAILED_REQUEST    = "失败请求"
	DUPLICATE_REQUEST = "重复请求"
)

type Config struct {
	Idx    int              // config index, start from 1
	Shards [NShards]int     // shard allocation
	Groups map[int][]string // server information
}

type ClientInfo struct {
	Uid int64
	Seq int64
}

type JoinRequest struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientInfo
}

type JoinResponse struct {
	RPCInfo string
}

type LeaveRequest struct {
	GIDs []int
	ClientInfo
}

type LeaveResponse struct {
	RPCInfo string
}

type Movement struct {
	Shard int
	GID   int
}

type MoveRequest struct {
	Movement
	ClientInfo
}

type MoveResponse struct {
	RPCInfo string
}

type QueryRequest struct {
	Idx int
	ClientInfo
}

type QueryResponse struct {
	Config
	RPCInfo string
}

type GeneralInput struct {
	OpType string
	ClientInfo
	Input interface{}
}

type GeneralOutput struct {
	Output  interface{}
	RPCInfo string
}

func (c Config) String() string {
	var str string
	var i int
	for i < NShards {
		gid := c.Shards[i]
		l := i
		for i < NShards && c.Shards[i] == gid {
			i++
		}
		r := i - 1
		if l == r {
			str += fmt.Sprintf("%d:%d ", gid, l)
		} else {
			str += fmt.Sprintf("%d:%d-%d ", gid, l, r)
		}
	}
	return fmt.Sprintf("CONF %d [%s]", c.Idx, str[:len(str)-1])
}

func (c ClientInfo) String() string {
	return fmt.Sprintf("[CTRL-CLI %d SEQ-%d]", c.Uid, c.Seq)
}

func (r GeneralInput) String() string {
	str := r.ClientInfo.String()
	switch v := r.Input.(type) {
	// Query
	case int:
		str += fmt.Sprintf("Query %d", v)

	// Join
	case map[int][]string:
		s := make([]int, 0)
		for g := range v {
			s = append(s, g)
		}
		str += fmt.Sprintf("Join %+v", s)

	// Leave
	case []int:
		str += fmt.Sprintf("Leave %+v", v)

	// Move
	case Movement:
		str += fmt.Sprintf("Move Shard %d to GID %d", v.Shard, v.GID)
	}

	return str
}
