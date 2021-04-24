package raft

import (
	"fmt"
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
	return &Trigger{
		StartTime: time.Now().UnixNano(),
		C:         make(chan int),
		On:        true,
	}
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
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Elapse()
	}
	/*-----------------------------------------*/
}

// close the Trigger in advance
// caller is within a critical section, no need to lock
func (rf *Raft) closeTrigger(st int64) {
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Close()
	}
}

// reset the Trigger
// caller is within a critical section, no need to lock
func (rf *Raft) resetTrigger() {
	if rf.trigger != nil && rf.trigger.On {
		rf.trigger.Close()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	Debug("========" + RAFT_FORMAT + "CRASHED========", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func TimerForTest(c chan int) {
	var t int
	var s string
outer:
	for {
		select {
		case <-c:
			break outer
		default:
			t++
			s += "*"
			time.Sleep(time.Second)
		}
		fmt.Printf("%02d second %s\n", t, s)
		if t >= 100 {
			panic("panic_too_long")
		}
	}
}
