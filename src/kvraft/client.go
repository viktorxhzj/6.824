package kvraft

import (
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	size    int
	ClerkId
	recentLeader int
	// You will have to modify this struct.
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.size = len(servers)
	ck.Uid = nrand()
	// fmt.Println("NEW CLIENT", ck.Uid)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	var req GetRequest
	var resp GetResponse

	req.Uid = ck.Uid
	req.Key = key
	req.Seq = atomic.AddInt64(&ck.Seq, 1)

	i := ck.recentLeader

	CDebug(ck.Uid, "开始GET%+v [NODE %d]", req, i)
	for {
		if ok := ck.servers[i].Call("KVServer.Get", &req, &resp); !ok {
			resp.RPCInfo = NETWORK_FAILURE
		}

		switch resp.RPCInfo {
		case NETWORK_FAILURE:
			i = (i + 1) % ck.size

		case WRONG_LEADER:
			i = (i + 1) % ck.size

		case SUCCESS:
			ck.recentLeader = i
			CDebug(ck.Uid, "成功GET%+v", req)
			return resp.Value

		case FAILED_REQUEST:
			i = (i + 1) % ck.size

		}
		resp.Value = ""
		resp.RPCInfo = ""
		CDebug(ck.Uid, "重试%+v [NODE %d]", req, i)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	var req PutAppendRequest
	var resp PutAppendResponse

	req.Uid = ck.Uid
	req.Key = key
	req.Value = value
	req.OpType = OpType(op)
	req.Seq = atomic.AddInt64(&ck.Seq, 1)

	i := ck.recentLeader

	CDebug(ck.Uid, "开始PUTAPPEND%+v [NODE %d]", req, i)
	for {
		if ok := ck.servers[i].Call("KVServer.PutAppend", &req, &resp); !ok {
			resp.RPCInfo = NETWORK_FAILURE
		}

		switch resp.RPCInfo {
		case NETWORK_FAILURE:
			i = (i + 1) % ck.size

		case WRONG_LEADER:
			i = (i + 1) % ck.size

		case SUCCESS:
			ck.recentLeader = i
			CDebug(ck.Uid, "成功PUTAPPEND%+v", req)
			return

		case FAILED_REQUEST:
			i = (i + 1) % ck.size

		case DUPLICATE_REQUEST:
			CDebug(ck.Uid, "幂等性校验未通过%+v", req)
			return

		}

		resp.RPCInfo = ""
		CDebug(ck.Uid, "重试%+v [NODE %d]", req, i)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
