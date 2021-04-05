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

func Debug(format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%s [NODE %d]",
		time.Now().Format("15:04:05.000"), 99)
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
