package raft

import (
	"fmt"
	"os"
	"time"
)

var (
	file          *os.File
	prefix        = "../logs/"
	suffix        = ".logger"
	enableDebug   = 1
	enableConsole = 0
	enableFile    = 1
)

func init() {
	if enableDebug == 1 && enableFile == 1 {
		filePath := prefix + time.Now().Format("15:04:05.000") + suffix
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}

func Debug(rf *Raft, format string, info ...interface{}) {
	if enableDebug == 0 {
		return
	}

	str := fmt.Sprintf("%s Apply=%d,Commit=%d,Term=%d,Off=%d, {...=>[%d|%d]}", 
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
	if enableFile == 1 {
		_, err := file.WriteString(str)
		if err != nil {
			print("writing to file")
			return
		}
	}

	if enableConsole == 1 {
		print(str)
	}
}
