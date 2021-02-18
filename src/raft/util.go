package raft

import (
	"6.824/logger"
	"sync/atomic"
	"time"
)

type Trigger struct {
	On        bool
	C         chan int
	StartTime int64
	Elapsed   bool
}

func NewTrigger() *Trigger {
	t := &Trigger{}
	t.StartTime = time.Now().UnixNano()
	t.C = make(chan int)
	t.On = true
	return t
}

func (t *Trigger) Wait() {
	<-t.C
}

func (t *Trigger) Elapse() {
	t.Elapsed = true
	t.On = false
	close(t.C)
}

func (t *Trigger) Close() {
	t.On = false
	close(t.C)
}

// Trigger naturally elapses
func (rf *Raft) elapseTrigger(d time.Duration, st int64) {
	time.Sleep(d)
	/*+++++++++++++++++++++++++++++++++++++++++*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 当且仅当st时刻触发的定时器还在时，使之过期
	if rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Elapse()
	}
	/*-----------------------------------------*/
}

// close the Trigger in advance
// caller is within a critical section, no need to lock
func (rf *Raft) closeTrigger(st int64) {
	if rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Close()
	}
}

// reset the Trigger
// caller is within a critical section, no need to lock
func (rf *Raft) resetTrigger() {
	if rf.trigger.On {
		rf.trigger.Close()
	}
}

func (rf *Raft) printLog() {
	logger.Debug(rf.me, "AppendEntries returns,logs=%+v", rf.logs)
}

// sliceIndex 找到某一日志行在当前日志切片所对应的索引
// 如果当前无日志，返回-2
// 如果该日志行不在日志切片中，返回-2
// 合法的返回结果为 [0, 1, ...]
func (rf *Raft) sliceIndex(logIndex int) int {
	if logIndex == 0 {
		return SliceIndexStart
	}

	l, r := 0, len(rf.logs)-1
	for l <= r {
		m := (l + r) / 2

		if rf.logs[m].Index > logIndex {
			r--
		} else if rf.logs[m].Index < logIndex {
			l++
		} else {
			return m
		}
	}
	return SliceIndexNotFound
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// lastLogInfo 返回最后一个日志行的信息。
// 调用者在临界区内，不上锁。
// 如果日志为空，返回 ZeroLogEntry 。
func (rf *Raft) lastLogInfo() (LogEntry, bool) {

	if len(rf.logs) == 0 {
		return ZeroLogEntry, false
	}

	return rf.logs[len(rf.logs)-1], true
}
