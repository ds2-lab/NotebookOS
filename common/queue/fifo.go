package queue

// Fifo implements a first-in first-out (FIFO) queue.
type Fifo[T any] struct {
	elements []T
}

// NewFifo creates a new Queue struct with the specified initial size/capacity
// and returns a pointer to it.
func NewFifo[T any](initialSize int) *Fifo[T] {
	if initialSize < 0 {
		initialSize = 1
	}

	return &Fifo[T]{
		elements: make([]T, 0, initialSize),
	}
}

// NewFifoFromSlice creates a new Queue struct from the specified slice.
func NewFifoFromSlice[T any](arr []T) *Fifo[T] {
	// Create a slice for the new Fifo.
	elements := make([]T, len(arr))

	// Copy the elements from the given slice into the elements slice.
	copy(elements, arr)

	// Create and return the Fifo queue.
	return &Fifo[T]{
		elements: elements,
	}
}

// ToSlice returns a copy of the target Fifo as a []T.
func (q *Fifo[T]) ToSlice() []T {
	slice := make([]T, q.Len())
	copy(slice, q.elements)
	return slice
}

// Enqueue adds the specified element to the queue.
func (q *Fifo[T]) Enqueue(elem T) {
	q.elements = append(q.elements, elem)
}

// Remove removes the specified element from anywhere in the queue.
//
// If the target element is found and removed, then it will be returned along with the boolean flag "true".
//
// If the target element is not found, then "false" is returned, along with the 'zero' value for the type parameter
// of the target Fifo.
func (q *Fifo[T]) Remove(target T, eq func(t1 T, t2 T) bool) (T, bool) {
	for idx, elem := range q.elements {
		if eq(target, elem) {
			q.elements = append(q.elements[:idx], q.elements[idx+1:]...)
			return target, true
		}
	}

	var zero T
	return zero, false
}

// Dequeue removes and returns the next element in the queue.
//
// If the length of the Queue is 0, then Dequeue will return nil.
func (q *Fifo[T]) Dequeue() (T, bool) {
	if len(q.elements) == 0 {
		var zero T
		return zero, false
	}

	elem := q.elements[0]
	q.elements = q.elements[1:]

	return elem, true
}

// Peek returns but does not remove the next element in the queue.
//
// If the length of the Queue is 0, then Peek will return nil.
func (q *Fifo[T]) Peek() (T, bool) {
	if len(q.elements) == 0 {
		var zero T
		return zero, false
	}

	return q.elements[0], true
}

// Len returns the number of elements in the queue.
func (q *Fifo[T]) Len() int {
	return len(q.elements)
}
