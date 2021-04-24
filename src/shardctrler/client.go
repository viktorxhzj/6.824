package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync/atomic"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	size         int
	recentLeader int32
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

	i := atomic.LoadInt32(&ck.recentLeader)

	// ck.Log("开始Query%+v", req)
	// defer ck.Log("成功Query%+v", req)

	for {
		// try each known server.
		for range ck.servers {
			var resp QueryResponse
			ck.servers[i].Call("ShardCtrler.Query", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&ck.recentLeader, i)
				return resp.Config
			}
			i = (i + 1) % int32(ck.size)
		}
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

	i := atomic.LoadInt32(&ck.recentLeader)

	ck.Log("开始Join%+v", req)
	defer ck.Log("成功Join%+v", req)
	for {
		// try each known server.
		for range ck.servers {
			var resp JoinResponse
			ck.servers[i].Call("ShardCtrler.Join", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				return
			}
			i = (i + 1) % int32(ck.size)
		}
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

	i := atomic.LoadInt32(&ck.recentLeader)

	ck.Log("开始Leave%+v", req)
	defer ck.Log("成功Leave%+v", req)
	for {
		// try each known server.
		for range ck.servers {
			var resp LeaveResponse
			ck.servers[i].Call("ShardCtrler.Leave", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				return
			}
			i = (i + 1) % int32(ck.size)
		}
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

	i := atomic.LoadInt32(&ck.recentLeader)

	ck.Log("开始Move%+v", req)
	defer ck.Log("成功Move%+v", req)

	for {
		// try each known server.
		for range ck.servers {
			var resp MoveResponse
			ck.servers[i].Call("ShardCtrler.Move", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				return
			}
			i = (i + 1) % int32(ck.size)
		}
	}
}
