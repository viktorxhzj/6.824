package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"time"
)

type Clerk struct {
	scc       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClerkId
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
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.scc = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.Uid = GenerateClerkId()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {

	ck.Seq++
	req := GetRequest{
		Key: key,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}
	shard := key2shard(key)
	ck.Log(ck.Uid, "开始%+v,分片%d", req, shard)
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			ck.Log(ck.Uid, "%+v轮询GID%d", req, gid)
		round:
			for si := 0; si < len(servers); si++ {
				var resp GetResponse
				srv := ck.make_end(servers[si])
				if ok := srv.Call("ShardKV.Get", &req, &resp); !ok {
					resp.RPCInfo = NETWORK_ERROR
				}
				resp.Key = req.Key
				resp.ClerkId = req.ClerkId
				ck.Log(ck.Uid, "%+v", resp)
				switch resp.RPCInfo {
				case SUCCEEDED_REQUEST:
					return resp.Value
				case WRONG_GROUP:
					break round
				default:
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		ck.config = ck.scc.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.Seq++
	req := PutAppendRequest{
		Key:   key,
		Value: value,
		ClerkId: ClerkId{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
		OpType: op,
	}
	shard := key2shard(key)
	ck.Log(ck.Uid, "开始%+v,分片%d", req, shard)

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.Log(ck.Uid, "%+v轮询GID%d", req, gid)
		round:
			for si := 0; si < len(servers); si++ {
				var resp PutAppendResponse
				srv := ck.make_end(servers[si])
				if ok := srv.Call("ShardKV.PutAppend", &req, &resp); !ok {
					resp.RPCInfo = NETWORK_ERROR
				}
				resp.Key = req.Key
				resp.OpType = req.OpType
				resp.ClerkId = req.ClerkId
				ck.Log(ck.Uid, "%+v", resp)
				switch resp.RPCInfo {
				case SUCCEEDED_REQUEST:
					return
				case DUPLICATE_REQUEST:
					return
				case WRONG_GROUP:
					break round
				default:

				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
		// ask controler for the latest configuration.
		ck.config = ck.scc.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
