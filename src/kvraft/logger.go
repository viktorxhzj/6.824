package kvraft

import (
	"fmt"
	"time"
)

var (
	EnableDebug   = 0
	EnableConsole = 0
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
	//write(str)
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
	//write(str)
}

func (ck *Clerk) Log(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [%s]",
		time.Now().Format("15:04:05.000"), ck.Uid)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	//write(str)
}
