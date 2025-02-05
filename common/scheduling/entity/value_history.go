package entity

import (
	"container/heap"
	"golang.org/x/exp/rand"
	"sync"
	"time"
)

type ValueHeap[T any] []*ValueEntry[T]

func (h ValueHeap[T]) Len() int {
	return len(h)
}

func (h ValueHeap[T]) Less(i, j int) bool {
	// log.Printf("Less %d, %d (%v, %v) of %d", i, j, h[i], h[j], len(h))
	return h[i].Compare(h[j]) < 0
}

func (h ValueHeap[T]) Swap(i, j int) {
	// log.Printf("Swap %d, %d (%v, %v) of %d", i, j, h[i], h[j], len(h))
	h[i].SetIdx(j)
	h[j].SetIdx(i)
	h[i], h[j] = h[j], h[i]
}

func (h *ValueHeap[T]) Push(x interface{}) {
	x.(*ValueEntry[T]).SetIdx(len(*h))
	*h = append(*h, x.(*ValueEntry[T]))
}

func (h *ValueHeap[T]) Pop() interface{} {
	old := *h
	n := len(old)
	ret := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return ret
}

func (h ValueHeap[T]) Peek() *ValueEntry[T] {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

func (h ValueHeap[T]) Seek(target *ValueEntry[T]) *ValueEntry[T] {
	return h.SeekFrom(target, 0, false)
}

func (h ValueHeap[T]) SeekFrom(target *ValueEntry[T], idx int, exclude bool) *ValueEntry[T] {
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

type ValueEntry[T any] struct {
	Value     T         `json:"value"`
	Timestamp time.Time `json:"timestamp"`

	Index int `json:"index"`
}

// GetIdx returns the Index of the ValueEntry.
func (entry *ValueEntry[T]) GetIdx() int {
	return entry.Index
}

// Idx returns the Index of the ValueEntry.
// Idx is just an alias for GetIdx.
func (entry *ValueEntry[T]) Idx() int {
	return entry.GetIdx()
}

// SetIdx sets the Index of the ValueEntry.
func (entry *ValueEntry[T]) SetIdx(index int) {
	entry.Index = index
}

// Compare compares the target ValueEntry with the specified ValueEntry.
//
// If entry's Timestamp occurs after other's Timestamp, then Compare returns a positive integer (i.e., entry > other).
// If entry's Timestamp is equal to other's Timestamp, then Compare returns 0 (i.e., entry == other).
// If entry's Timestamp occurs before other's Timestamp, then Compare returns a negative integer (i.e, entry < other).
func (entry *ValueEntry[T]) Compare(other *ValueEntry[T]) int {
	return entry.Timestamp.Compare(other.Timestamp)
}

// ValueHistory is a min-heap of values -- generally some sort of metric or statistic -- sorted by timestamp.
// This maintains the history of the different values.
// The type of the value can be anything.
type ValueHistory[T any] struct {
	// ValueName denotes the name of the metric or variable whose history is being tracked.
	ValueName string `json:"value_name"`

	// ValueType denotes the String name of the type of the target metric or variable.
	ValueType string `json:"value_type"`

	// This is a min-heap sorted by Timestamp of the values inserted into the history.
	Entries ValueHeap[T] `json:"value_entries"`

	// This is the most recent value added to the history.
	LatestValue T `json:"latest_value"`

	mu sync.Mutex
}

func NewValueHistory[T any](name string, typeName string) *ValueHistory[T] {
	return &ValueHistory[T]{
		ValueName: name,
		ValueType: typeName,
		Entries:   make(ValueHeap[T], 0),
	}
}

func (h *ValueHistory[T]) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.Entries)
}

func (h *ValueHistory[T]) AddValue(value T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry := &ValueEntry[T]{
		Value:     value,
		Timestamp: time.Now(),
	}

	heap.Push(&h.Entries, entry)

	h.LatestValue = value
}
