package shardkv

import (
	"fmt"
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

var (
	EnableDebug   = 1
	EnableConsole = 1
	EnableFile    = 1
)

func (kv *ShardKV) Log(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := time.Now().Format("15:04:05.000")
	str += fmt.Sprintf(" Conf%d->%d,Step:%d,", kv.oldConf.Num, kv.conf.Num, kv.step)
	var his []ShardInfo
	for i := 0; i < shardctrler.NShards; i++ {
		for k := range kv.historyState[i] {
			his = append(his, ShardInfo{Shard: i, ConfigNum: k})
		}
	}
	str += fmt.Sprintf("His%+v", his)
	str += fmt.Sprintf("[GROUP %d]  ", kv.gid)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

// 可不在临界区
func (kv *ShardKV) Debug(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := time.Now().Format("15:04:05.000")
	str += fmt.Sprintf("[GROUP %d]  ", kv.gid)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

func (ck *Clerk) Log(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [%s]",
		time.Now().Format("15:04:05.000"), ck.Uid)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

func write(str string) {
	if EnableFile == 1 {
		_, err := raft.File.WriteString(str)
		if err != nil {
			panic("Failed to write")
		}
	}
	if EnableConsole == 1 {
		print(str)
	}
}