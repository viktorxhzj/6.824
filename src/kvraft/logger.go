package kvraft

import (
	"fmt"
	"time"

	"6.824/raft"
)

var (
	EnableDebug   = 0
	EnableConsole = 1
	EnableFile    = 0
)

func (kv *KVServer) Log(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [KV %d]",
		time.Now().Format("15:04:05.000"), kv.me)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

// 可不在临界区
func (kv *KVServer) Debug(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [KV %d]",
		time.Now().Format("15:04:05.000"), kv.me)
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
