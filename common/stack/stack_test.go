package stack_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/stack"
)

var _ = Describe("Stack", func() {
	var intStack *stack.Stack[int]
	var stringStack *stack.Stack[string]

	BeforeEach(func() {
		intStack = &stack.Stack[int]{}
		stringStack = &stack.Stack[string]{}
	})

	Describe("Push", func() {
		It("should add elements to the stack", func() {
			intStack.Push(10)
			intStack.Push(20)
			Expect(intStack.Size()).To(Equal(2))

			stringStack.Push("hello")
			stringStack.Push("world")
			Expect(stringStack.Size()).To(Equal(2))
		})
	})

	Describe("Pop", func() {
		It("should remove and return the top element of the stack", func() {
			intStack.Push(10)
			intStack.Push(20)

			value, err := intStack.Pop()
			Expect(err).ToNot(HaveOccurred())
			Expect(value).To(Equal(20))
			Expect(intStack.Size()).To(Equal(1))

			value, err = intStack.Pop()
			Expect(err).ToNot(HaveOccurred())
			Expect(value).To(Equal(10))
			Expect(intStack.IsEmpty()).To(BeTrue())
		})

		It("should return an error if the stack is empty", func() {
			value, err := intStack.Pop()
			Expect(err).To(HaveOccurred())
			Expect(value).To(BeZero())
		})
	})

	Describe("Peek", func() {
		It("should return the top element without removing it", func() {
			stringStack.Push("hello")
			stringStack.Push("world")

			value, err := stringStack.Peek()
			Expect(err).ToNot(HaveOccurred())
			Expect(value).To(Equal("world"))
			Expect(stringStack.Size()).To(Equal(2))
		})

		It("should return an error if the stack is empty", func() {
			value, err := stringStack.Peek()
			Expect(err).To(HaveOccurred())
			Expect(value).To(BeZero())
		})
	})

	Describe("IsEmpty", func() {
		It("should return true if the stack is empty", func() {
			Expect(intStack.IsEmpty()).To(BeTrue())
			intStack.Push(10)
			Expect(intStack.IsEmpty()).To(BeFalse())
		})
	})

	Describe("Size", func() {
		It("should return the correct number of elements in the stack", func() {
			Expect(intStack.Size()).To(Equal(0))
			intStack.Push(10)
			intStack.Push(20)
			Expect(intStack.Size()).To(Equal(2))

			_, _ = intStack.Pop()
			Expect(intStack.Size()).To(Equal(1))
		})
	})
})
