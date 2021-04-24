package shardctrler

//
// Shardctrler clerk.
//

import (
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Client struct {
	servers      []*labrpc.ClientEnd
	size         int
	recentLeader int32
	ClientInfo
}

const CLIENT_REQUEST_INTERVAL = 100 * time.Millisecond

func MakeClient(servers []*labrpc.ClientEnd) *Client {
	c := new(Client)
	c.servers = servers
	c.size = len(servers)
	c.Uid = generateClientId()
	return c
}

func (c *Client) Query(num int) Config {

	req := QueryRequest{
		Idx: num,
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&c.recentLeader)

	c.info("开始Query %+v", req)
	for {
		for range c.servers {
			var resp QueryResponse
			c.servers[i].Call("ShardCtrler.Query", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&c.recentLeader, i)
				c.info("成功Query %+v", resp)
				return resp.Config
			}
			i = (i + 1) % int32(c.size)
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (c *Client) Join(servers map[int][]string) {
	req := JoinRequest{
		Servers: servers,
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&c.recentLeader)

	c.info("开始Join %+v", req)
	defer c.info("成功Join %+v", req)
	for {
		for range c.servers {
			var resp JoinResponse
			c.servers[i].Call("ShardCtrler.Join", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&c.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(c.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (c *Client) Leave(gids []int) {
	req := LeaveRequest{
		GIDs: gids,
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&c.recentLeader)

	c.info("开始Leave %+v", req)
	defer c.info("成功Leave %+v", req)
	for {
		for range c.servers {
			var resp LeaveResponse
			c.servers[i].Call("ShardCtrler.Leave", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&c.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(c.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (c *Client) Move(shard int, gid int) {
	req := MoveRequest{
		Movement: Movement{
			Shard: shard,
			GID:   gid,
		},
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&c.recentLeader)

	c.info("开始Move %+v", req)
	defer c.info("成功Move %+v", req)
	for {
		for range c.servers {
			var resp MoveResponse
			c.servers[i].Call("ShardCtrler.Move", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&c.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(c.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

var (
	ctrlerClientGlobalId int64 // monotonically increasing for convenience
)

func generateClientId() int64 {
	return atomic.AddInt64(&ctrlerClientGlobalId, 1)
}
