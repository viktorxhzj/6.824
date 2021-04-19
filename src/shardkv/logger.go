package shardkv

import (
	"fmt"
	"time"

	"6.824/raft"
)

var (
	EnableDebug   = 1
	EnableConsole = 0
	EnableFile    = 0
)

// 可不在临界区打印
func Debug(node int, format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [KV %d]",
		time.Now().Format("15:04:05.000"), node)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

// 必须由临界区保护
func (kv *ShardKV) Log(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v",
		time.Now().Format("15:04:05.000"))
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
	if EnableFile == 1 && raft.EnableFile == 1 {
		_, err := raft.File.WriteString(str)
		if err != nil {
			panic("Failed to write")
		}
	}
	if EnableConsole == 1 {
		print(str)
	}
}