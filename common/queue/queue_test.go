package queue_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scusemua/distributed-notebook/common/queue"
)

var _ = Describe("Queue Tests", func() {
	It("Will create a new, empty queue correctly", func() {
		q := queue.NewQueue(1)
		Expect(q).ToNot(BeNil())
		Expect(q.Len()).To(Equal(0))
		Expect(q.Dequeue()).To(BeNil())
	})

	It("Will handle a single enqueue and dequeue operation correctly", func() {
		q := queue.NewQueue(1)
		Expect(q).ToNot(BeNil())

		q.Enqueue("element")
		Expect(q.Len()).To(Equal(1))

		Expect(q.Peek()).To(Equal("element"))

		val := q.Dequeue()
		Expect(val).ToNot(BeNil())

		elem, ok := val.(string)
		Expect(ok).To(BeTrue())
		Expect(elem).ToNot(BeNil())
		Expect(elem).To(Equal("element"))
	})

	It("Will handle a series of 'enqueue' operations followed by a series of 'dequeue' operations", func() {
		q := queue.NewQueue(1)
		alphabet := "abcdefghijklmnopqrstuvwxyz"

		for i := 0; i < len(alphabet); i++ {
			letter := alphabet[i : i+1]
			q.Enqueue(letter)
			Expect(q.Len()).To(Equal(i + 1))
			Expect(q.Peek()).To(Equal("a"))
		}

		Expect(q.Len()).To(Equal(len(alphabet)))

		length := len(alphabet)
		for i := 0; i < len(alphabet); i++ {
			Expect(q.Len()).To(Equal(length))

			val := q.Peek()
			Expect(val).ToNot(BeNil())

			str, ok := val.(string)
			Expect(ok).To(BeTrue())

			Expect(str).To(Equal(alphabet[i : i+1]))

			val = q.Dequeue()
			Expect(val).ToNot(BeNil())

			Expect(val.(string)).To(Equal(str))

			length -= 1
		}
	})

	It("Will correctly handle a series of intermingled 'enqueue' and 'dequeue' operations", func() {
		q := queue.NewQueue(1)

		q.Enqueue("a")
		q.Enqueue("b")
		q.Enqueue("c")

		Expect(q.Len()).To(Equal(3))
		Expect(q.Peek().(string)).To(Equal("a"))

		val := q.Dequeue()
		Expect(val.(string)).To(Equal("a"))
		Expect(q.Peek().(string)).To(Equal("b"))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("b"))
		Expect(q.Peek().(string)).To(Equal("c"))

		q.Enqueue("d")
		q.Enqueue("e")

		Expect(q.Len()).To(Equal(3))
		Expect(q.Peek().(string)).To(Equal("c"))

		q.Enqueue("f")
		Expect(q.Len()).To(Equal(4))
		Expect(q.Peek().(string)).To(Equal("c"))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("c"))
		Expect(q.Peek().(string)).To(Equal("d"))
		Expect(q.Len()).To(Equal(3))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("d"))
		Expect(q.Peek().(string)).To(Equal("e"))
		Expect(q.Len()).To(Equal(2))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("e"))
		Expect(q.Peek().(string)).To(Equal("f"))
		Expect(q.Len()).To(Equal(1))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("f"))
		Expect(q.Peek()).To(BeNil())
		Expect(q.Len()).To(Equal(0))

		val = q.Dequeue()
		Expect(val).To(BeNil())

		q.Enqueue("g")
		Expect(q.Len()).To(Equal(1))
		Expect(q.Peek().(string)).To(Equal("g"))

		val = q.Dequeue()
		Expect(val.(string)).To(Equal("g"))
		Expect(q.Len()).To(Equal(0))
	})
})
