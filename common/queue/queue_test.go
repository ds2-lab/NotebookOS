package queue_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scusemua/distributed-notebook/common/queue"
)

var _ = Describe("Queue Tests", func() {
	It("Will create a new, empty queue correctly", func() {
		q := queue.NewFifo[string](1)
		Expect(q).ToNot(BeNil())
		Expect(q.Len()).To(Equal(0))

		val, ok := q.Dequeue()
		Expect(ok).To(BeFalse())
		Expect(val).To(Equal(""))
	})

	It("Will handle a single enqueue and dequeue operation correctly", func() {
		q := queue.NewFifo[string](1)
		Expect(q).ToNot(BeNil())

		q.Enqueue("element")
		Expect(q.Len()).To(Equal(1))

		val, ok := q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("element"))

		elem, ok := q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(elem).ToNot(BeNil())
		Expect(elem).To(Equal("element"))
	})

	It("Will handle a series of 'enqueue' operations followed by a series of 'dequeue' operations", func() {
		q := queue.NewFifo[string](1)
		alphabet := "abcdefghijklmnopqrstuvwxyz"

		for i := 0; i < len(alphabet); i++ {
			letter := alphabet[i : i+1]
			q.Enqueue(letter)
			Expect(q.Len()).To(Equal(i + 1))

			val, ok := q.Peek()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal("a"))
		}

		Expect(q.Len()).To(Equal(len(alphabet)))

		length := len(alphabet)
		for i := 0; i < len(alphabet); i++ {
			Expect(q.Len()).To(Equal(length))

			val, ok := q.Dequeue()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(alphabet[i : i+1]))

			length -= 1
		}
	})

	It("Will correctly handle a series of intermingled 'enqueue' and 'dequeue' operations", func() {
		q := queue.NewFifo[string](1)

		q.Enqueue("a")
		q.Enqueue("b")
		q.Enqueue("c")

		Expect(q.Len()).To(Equal(3))
		val, ok := q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("a"))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("a"))

		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("b"))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("b"))

		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("c"))

		q.Enqueue("d")
		q.Enqueue("e")

		Expect(q.Len()).To(Equal(3))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("c"))

		q.Enqueue("f")
		Expect(q.Len()).To(Equal(4))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("c"))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("c"))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("d"))
		Expect(q.Len()).To(Equal(3))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("d"))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("e"))
		Expect(q.Len()).To(Equal(2))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("e"))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("f"))
		Expect(q.Len()).To(Equal(1))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("f"))
		val, ok = q.Peek()
		Expect(ok).To(BeFalse())
		Expect(val).To(Equal(""))
		Expect(q.Len()).To(Equal(0))

		val, ok = q.Dequeue()
		Expect(ok).To(BeFalse())
		Expect(val).To(Equal(""))

		q.Enqueue("g")
		Expect(q.Len()).To(Equal(1))
		val, ok = q.Peek()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("g"))

		val, ok = q.Dequeue()
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("g"))
		Expect(q.Len()).To(Equal(0))
	})
})
