package shardctrler

//
// Shardctrler clerk.
//

import (
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	size         int
	ClerkId
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.size = len(servers)
	ck.Uid = GenerateClerkId()
	return ck
}

func (ck *Clerk) Query(num int) Config {

	req := QueryRequest{
		Num: num,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			ck.Log("开始Query%+v [NODE %d]", req, i)
			var resp QueryResponse
			srv.Call("ShardCtrler.Query", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.Log("成功Query%+v", req)
				return resp.Config
			}
		}
		// time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	req := JoinRequest{
		Servers: servers,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			ck.Log("开始Join%+v [NODE %d]", req, i)
			var resp JoinResponse
			srv.Call("ShardCtrler.Join", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				
				time.Sleep(CLIENT_REQUEST_INTERVAL)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log("幂等拦截%+v", req)
				return
			}
		}
		// time.Sleep(CLIENT_REQUEST_INTERVAL * 2)
	}
}

func (ck *Clerk) Leave(gids []int) {
	req := LeaveRequest{
		GIDs: gids,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			ck.Log("开始Leave%+v [NODE %d]", req, i)
			var resp LeaveResponse
			srv.Call("ShardCtrler.Leave", &req, &resp)
			if resp.RPCInfo == SUCCESS {
//				ck.Log("成功Leave%+v", req)

				time.Sleep(CLIENT_REQUEST_INTERVAL)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log("幂等拦截%+v", req)
				return
			}
		}
		// time.Sleep(CLIENT_REQUEST_INTERVAL * 2)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	req := MoveRequest{
		Movable: Movable{
			Shard: shard,
			GID:   gid,
		},
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			ck.Log("开始Move%+v [NODE %d]", req, i)
			var resp MoveResponse
			srv.Call("ShardCtrler.Move", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.Log("成功Move%+v", req)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log("幂等拦截%+v", req)
				return
			}
		}
		// time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}
