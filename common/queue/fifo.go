package queue

// FifoQueue implements a first-in first-out (FIFO) queue.
type FifoQueue struct {
	elements []interface{}
}

// NewQueue creates a new Queue struct with the specified initial size/capacity
// and returns a pointer to it.
func NewQueue(initialSize int) *FifoQueue {
	if initialSize < 0 {
		initialSize = 1
	}

	return &FifoQueue{
		elements: make([]interface{}, 0, initialSize),
	}
}

// Enqueue adds the specified element to the queue.
func (q *FifoQueue) Enqueue(elem interface{}) {
	q.elements = append(q.elements, elem)
}

// Dequeue removes and returns the next element in the queue.
//
// If the length of the Queue is 0, then Dequeue will return nil.
func (q *FifoQueue) Dequeue() interface{} {
	if len(q.elements) == 0 {
		return nil
	}

	elem := q.elements[0]
	q.elements = q.elements[1:]

	return elem
}

// Peek returns but does not remove the next element in the queue.
//
// If the length of the Queue is 0, then Peek will return nil.
func (q *FifoQueue) Peek() interface{} {
	if len(q.elements) == 0 {
		return nil
	}

	return q.elements[0]
}

// Len returns the number of elements in the queue.
func (q *FifoQueue) Len() int {
	return len(q.elements)
}
