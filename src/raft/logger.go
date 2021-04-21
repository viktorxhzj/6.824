package raft

import (
	"fmt"
	"os"
	"time"
)

var (
	File          *os.File
	Prefix        = "../logs/"
	Suffix        = ".logger"
	EnableDebug   = 1
	EnableConsole = 0
	EnableFile    = 0
)

func init() {
	if EnableDebug == 1 {
		filePath := Prefix + time.Now().Format("15:04:05.000") + Suffix
		File, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}
func Debug(rf *Raft, format string, info ...interface{}) {
	if EnableDebug == 0 {
		return
	}
	str := fmt.Sprintf("%s A=%d,C=%d,T=%d,O=%d,{...=>[%d|%d]}",
		time.Now().Format("15:04:05.000"), rf.lastAppliedIndex, rf.commitIndex, rf.currentTerm, rf.offset, rf.lastIncludedIndex, rf.lastIncludedTerm)

	if len(rf.logs) == 0 {
		str += "{}, "
	} else {
		str += fmt.Sprintf("{%+v->%+v}", rf.logs[0], rf.logs[len(rf.logs)-1])
	}
	str += fmt.Sprintf(" [NODE %d]", rf.me)
	str += fmt.Sprintf(format, info...)
	str += "\n"
	write(str)
}
func write(str string) {
	if EnableFile == 1 {
		_, err := File.WriteString(str)
		if err != nil {
			panic("Failed to write")
		}
	}
	if EnableConsole == 1 {
		print(str)
	}
}
