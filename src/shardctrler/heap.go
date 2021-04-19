package shardctrler

type Heap []int

func (h *Heap) Len() int           { return len(*h) }
func (h *Heap) Less(i, j int) bool { return (*h)[i] < (*h)[j] }
func (h *Heap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *Heap) Push(x interface{}) { *h = append(*h, x.(int)) }

func (h *Heap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n-1]
	*h = (*h)[0 : n-1]
	return x
}
