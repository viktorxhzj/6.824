package raft

import (
	"fmt"
	"time"

	"6.824/logger"
)

const (
	LOG_INFO  = "[INFO]"
	LOG_ERROR = "[ERROR]"
	LOG_DEBUG = "[DEBUG]"
)

const (
	RAFT_FORMAT = "[RAFT %d] "
)

var (
	LogFile    = 0
	LogConsole = 1
	InDebug    = 1
)

func init() {
	if LogFile == 1 {
		logger.EnableLogger()
	}
}

func (rf *Raft) info(format string, info ...interface{}) {
	rf.log(LOG_INFO, format, info...)
}

func (rf *Raft) error(format string, info ...interface{}) {
	rf.log(LOG_ERROR, format, info...)
	if InDebug == 1 {
		panic(LOG_ERROR)
	}
}

func (rf *Raft) log(prefix, format string, info ...interface{}) {
	if LogFile == 1 || LogConsole == 1 {
		msg := fmt.Sprintf("%s A=%d,C=%d,T=%d,O=%d,{...=>[%d|%d]}",
			time.Now().Format("15:04:05.000"), rf.lastAppliedIndex, rf.commitIndex, rf.currentTerm, rf.offset, rf.lastIncludedIndex, rf.lastIncludedTerm)

		if len(rf.logs) == 0 {
			msg += "{} "
		} else {
			msg += fmt.Sprintf("{%+v->%+v} ", rf.logs[0], rf.logs[len(rf.logs)-1])
		}
		msg += fmt.Sprintf(RAFT_FORMAT, rf.me)
		msg += fmt.Sprintf(format, info...)
		msg += "\n"
		if LogFile == 1 {
			logger.Write(msg)
		}
		if LogConsole == 1 {
			print(msg)
		}
	}
}

func Debug(format string, info ...interface{}) {
	if LogFile == 1 || LogConsole == 1 {
		msg := fmt.Sprintf("%s %v ", LOG_DEBUG, time.Now().Format("15:04:05.000"))
		msg += fmt.Sprintf(format, info...)
		msg += "\n"
		if LogFile == 1 {
			logger.Write(msg)
		}
		if LogConsole == 1 {
			print(msg)
		}
	}
}
