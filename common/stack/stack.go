package stack

import "fmt"

// Stack represents a stack data structure.
type Stack[T any] struct {
	elements []T
}

// Push adds an element to the top of the stack
func (s *Stack[T]) Push(element T) {
	s.elements = append(s.elements, element)
}

// Pop removes and returns the top element of the stack. It returns an error if the stack is empty.
func (s *Stack[T]) Pop() (T, error) {
	if len(s.elements) == 0 {
		var zero T
		return zero, fmt.Errorf("stack is empty")
	}
	topIndex := len(s.elements) - 1
	element := s.elements[topIndex]
	s.elements = s.elements[:topIndex]
	return element, nil
}

// Peek returns the top element of the stack without removing it. It returns an error if the stack is empty.
func (s *Stack[T]) Peek() (T, error) {
	if len(s.elements) == 0 {
		var zero T
		return zero, fmt.Errorf("stack is empty")
	}
	return s.elements[len(s.elements)-1], nil
}

// IsEmpty checks if the stack is empty
func (s *Stack[T]) IsEmpty() bool {
	return len(s.elements) == 0
}

// Size returns the number of elements in the stack
func (s *Stack[T]) Size() int {
	return len(s.elements)
}
