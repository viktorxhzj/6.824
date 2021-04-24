package shardctrler

import (
	"testing"
)

func Test_JoinLeave(t *testing.T) {
	sc := new(ShardCtrler)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	n := 10
	for i := 0; i < n; i++ {
		j := 500+i
		m := map[int][]string{j:{}}
		sc.joinGroups(m)
	}

	for i := 0; i < n; i++ {
		j := 500+i
		s := []int{j}
		sc.leaveGroups(s)
	}

}
