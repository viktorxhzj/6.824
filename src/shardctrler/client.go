package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"time"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	size         int
	recentLeader int
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

	ck.Seq++
	req := QueryRequest{
		Num: num,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		// try each known server.
		for range ck.servers {
			ck.Log(ck.Uid, "开始Query%+v [NODE %d]", req, i)
			var resp QueryResponse
			ck.servers[i].Call("ShardCtrler.Query", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				ck.Log(ck.Uid, "成功Query%+v", req)
				return resp.Config
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	ck.Seq++
	req := JoinRequest{
		Servers: servers,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		// try each known server.
		for range ck.servers {
			ck.Log(ck.Uid, "开始Join%+v [NODE %d]", req, i)
			var resp JoinResponse
			ck.servers[i].Call("ShardCtrler.Join", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				ck.Log(ck.Uid, "成功Join%+v", req)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log(ck.Uid, "幂等拦截%+v", req)
				return
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.Seq++
	req := LeaveRequest{
		GIDs: gids,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		// try each known server.
		for range ck.servers {
			ck.Log(ck.Uid, "开始Leave%+v [NODE %d]", req, i)
			var resp LeaveResponse
			ck.servers[i].Call("ShardCtrler.Leave", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				ck.Log(ck.Uid, "成功Leave%+v", req)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log(ck.Uid, "幂等拦截%+v", req)
				return
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Seq++
	req := MoveRequest{
		Movable: Movable{
			Shard: shard,
			GID:   gid,
		},
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		// try each known server.
		for range ck.servers {
			ck.Log(ck.Uid, "开始Move%+v [NODE %d]", req, i)
			var resp MoveResponse
			ck.servers[i].Call("ShardCtrler.Move", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				ck.Log(ck.Uid, "成功Move%+v", req)
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				ck.Log(ck.Uid, "幂等拦截%+v", req)
				return
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}
