package kvraft

import (
	"fmt"
	"time"

	"6.824/raft"
)

var (
	EnableDebug   = 1
	EnableConsole = 0
	EnableFile    = 1
)

func Debug(node int, format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [NODE %d]",
		time.Now().Format("15:04:05.000"), node)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}

func CDebug(client int64, format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%v [CLIENT %d]",
		time.Now().Format("15:04:05.000"), client)
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
