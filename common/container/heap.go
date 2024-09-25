package container

import (
	"math/rand"
)

type Comparable interface {
	// Compare compares the object with specified object.
	// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
	Compare(interface{}) float64
}

type HeapElement interface {
	Comparable

	SetIdx(int)

	String() string
}

type Heap []HeapElement

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	// fmt.Printf("Less %d, %d (%v, %v) of %d\n", i, j, h[i], h[j], len(h))
	return h[i].Compare(h[j]) < 0
}

func (h Heap) Swap(i, j int) {
	// fmt.Printf("Swap %d, %d (%v, %v) of %d\n", i, j, h[i], h[j], len(h))
	h[i].SetIdx(j)
	h[j].SetIdx(i)
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	x.(HeapElement).SetIdx(len(*h))
	*h = append(*h, x.(HeapElement))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	ret := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]

	// fmt.Printf("Popped value %v off of heap.\n", ret)
	return ret
}

func (h Heap) Peek() HeapElement {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

func (h Heap) Seek(target interface{}) HeapElement {
	return h.SeekFrom(target, 0, false)
}

func (h Heap) SeekFrom(target interface{}, idx int, exclude bool) HeapElement {
	if 0 >= len(h) {
		return nil
	}
	i, j := idx, idx
	if exclude {
		i, j = i*2+1, i*2+2
	}
	for ; i < len(h) || j < len(h); i, j = i*2+1, i*2+2 {
		smallJ := j != i && j < len(h) && h.Less(j, i)
		if smallJ && h[j].Compare(target) >= 0.0 {
			return h[j]
		} else if h[i].Compare(target) >= 0.0 {
			return h[i]
		}

		// Randomly select a branch
		if rand.Intn(2) == 1 {
			i = j
		}
	}
	return nil
}
