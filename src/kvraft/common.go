package kvraft

type (
	RPCInfo int
	OpType  string
)

const (
	GET    OpType = "Get"
	PUT    OpType = "Put"
	APPEND OpType = "Append"
	NIL    OpType = "NIL"

	SUCCESS           RPCInfo = 0
	NETWORK_FAILURE   RPCInfo = 1
	WRONG_LEADER      RPCInfo = 2
	FAILED_REQUEST    RPCInfo = 3
	DUPLICATE_REQUEST RPCInfo = 4

	NO_OP_INTERVAL = 1000
)

type Err string

type ClerkId struct {
	Uid int64
	Seq int64
}

//
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

type RaftResponse struct {
	Value string
	OpType
	RPCInfo
}
