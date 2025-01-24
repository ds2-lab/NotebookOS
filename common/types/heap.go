package types

type Comparable interface {
	// Compare compares the object with specified object.
	// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
	Compare(interface{}) float64
}

type HeapElement interface {
	Comparable

	SetIdx(HeapElementMetadataKey, int)

	GetIdx(HeapElementMetadataKey) int

	String() string

	SetMeta(HeapElementMetadataKey, interface{})
}

type HeapElementMetadataKey string

func (k HeapElementMetadataKey) String() string {
	return string(k)
}

type Heap struct {
	Elements    []HeapElement
	MetadataKey HeapElementMetadataKey
}

func NewHeap(metadataKey HeapElementMetadataKey) *Heap {
	return &Heap{
		Elements:    make([]HeapElement, 0),
		MetadataKey: metadataKey,
	}
}

func (h *Heap) Len() int {
	return len(h.Elements)
}

func (h *Heap) Less(i, j int) bool {
	// fmt.Printf("Less %d, %d (%v, %v) of %d\n", i, j, h[i], h[j], len(h))
	return h.Elements[i].Compare(h.Elements[j]) < 0
}

func (h *Heap) Swap(i, j int) {
	// fmt.Printf("Swap %d, %d (%v, %v) of %d\n", i, j, h[i], h[j], len(h))
	h.Elements[i].SetIdx(h.MetadataKey, j)
	h.Elements[j].SetIdx(h.MetadataKey, i)

	h.Elements[i].SetMeta(h.MetadataKey, int32(j))
	h.Elements[j].SetMeta(h.MetadataKey, int32(i))

	h.Elements[i], h.Elements[j] = h.Elements[j], h.Elements[i]
}

func (h *Heap) Push(x interface{}) {
	x.(HeapElement).SetIdx(h.MetadataKey, len(h.Elements))
	x.(HeapElement).SetMeta(h.MetadataKey, int32(len(h.Elements)))
	h.Elements = append(h.Elements, x.(HeapElement))
}

func (h *Heap) Pop() interface{} {
	old := h.Elements
	n := len(old)
	ret := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.Elements = old[0 : n-1]

	// fmt.Printf("Popped value %v off of heap.\n", ret)
	return ret
}

func (h *Heap) Peek() HeapElement {
	if len(h.Elements) == 0 {
		return nil
	}
	return h.Elements[0]
}

//func (h Heap) Seek(target interface{}) HeapElement {
//	return h.SeekFrom(target, 0, false)
//}
//
//func (h Heap) SeekFrom(target interface{}, idx int, exclude bool) HeapElement {
//	if 0 >= len(h) {
//		return nil
//	}
//	i, j := idx, idx
//	if exclude {
//		i, j = i*2+1, i*2+2
//	}
//	for ; i < len(h) || j < len(h); i, j = i*2+1, i*2+2 {
//		smallJ := j != i && j < len(h) && h.Less(j, i)
//		if smallJ && h[j].Compare(target) >= 0.0 {
//			return h[j]
//		} else if h[i].Compare(target) >= 0.0 {
//			return h[i]
//		}
//
//		// Randomly select a branch
//		if rand.Intn(2) == 1 {
//			i = j
//		}
//	}
//	return nil
//}
