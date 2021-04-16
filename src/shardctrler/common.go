package shardctrler

import "fmt"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
	NIL   = "NIL"

	SUCCESS           = "成功"
	NETWORK_FAILURE   = "超时"
	WRONG_LEADER      = "非法领袖"
	FAILED_REQUEST    = "失败重试"
	DUPLICATE_REQUEST = "幂等拦截"

	NO_OP_INTERVAL = 1000
)

type ClerkId struct {
	Uid int64
	Seq int64
}

func (c ClerkId) String() string {
	return fmt.Sprintf("[CLI-%d SEQ-%d]", c.Uid, c.Seq)
}

type Err string

type JoinRequest struct {
	Servers map[int][]string // new GID -> servers mappings
	ClerkId
}

type JoinResponse struct {
	RPCInfo string
}

type LeaveRequest struct {
	GIDs []int
	ClerkId
}

type LeaveResponse struct {
	RPCInfo string
}

type Movable struct {
	Shard int
	GID   int
}

type MoveRequest struct {
	Movable
	ClerkId
}

type MoveResponse struct {
	RPCInfo string
}

type QueryRequest struct {
	Num int // desired config number
	ClerkId
}

type QueryResponse struct {
	Config
	RPCInfo string
}

type RaftRequest struct {
	OpType string
	ClerkId
	Input interface{}
}

type RaftResponse struct {
	Output interface{}
	RPCInfo string
}
