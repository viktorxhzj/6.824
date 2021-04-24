package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

type Client struct {
	mu       sync.Mutex
	scc      *shardctrler.Client
	conf     shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientInfo
}

const CLIENT_REQUEST_INTERVAL = 100 * time.Millisecond // 客户端发起新一轮请求的间隔时间

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Client {
	ck := new(Client)
	ck.scc = shardctrler.MakeClient(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.Uid = generateclientId()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (c *Client) Get(key string) string {
	req := GetRequest{
		Key: key,
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
	}
	s := key2shard(key)
	c.info("开始Get %+v", req)
	defer c.info("成功Get %+v", req)
	for {
		c.mu.Lock()
		gid := c.conf.Shards[s]
		servers, ok := c.conf.Groups[gid]
		c.mu.Unlock()
		if ok {
		round:
			for _, srvi := range servers {
				var resp GetResponse
				srv := c.make_end(srvi)
				srv.Call("ShardKV.Get", &req, &resp)
				resp.Key = req.Key
				switch resp.RPCInfo {
				case SUCCESS:
					return resp.Value
				case WRONG_GROUP:
					break round
				default:
				}
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		config := c.scc.Query(-1)
		c.mu.Lock()
		c.conf = config
		c.mu.Unlock()
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (c *Client) PutAppend(key string, value string, op string) {

	req := PutAppendRequest{
		Key:   key,
		Value: value,
		ClientInfo: ClientInfo{
			Uid: c.Uid,
			Seq: atomic.AddInt64(&c.Seq, 1),
		},
		OpType: op,
	}
	s := key2shard(key)
	c.info("开始PutAppend %+v", req)
	defer c.info("成功PutAppend %+v", req)
	for {
		c.mu.Lock()
		gid := c.conf.Shards[s]
		servers, ok := c.conf.Groups[gid]
		c.mu.Unlock()
		if ok {
		round:
			for _, srvi := range servers {
				var resp PutAppendResponse
				srv := c.make_end(srvi)
				srv.Call("ShardKV.PutAppend", &req, &resp)
				switch resp.RPCInfo {
				case SUCCESS:
					return
				case DUPLICATE_REQUEST:
					return
				case WRONG_GROUP:
					break round
				default:

				}
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		config := c.scc.Query(-1)
		c.mu.Lock()
		c.conf = config
		c.mu.Unlock()
	}
}

func (ck *Client) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Client) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

var (
	kvClientGlobalId int64
)

func generateclientId() int64 {
	return atomic.AddInt64(&kvClientGlobalId, 1)
}
