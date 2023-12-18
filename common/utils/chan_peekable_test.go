package utils

import (
	"runtime"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ChanPeekable", func() {
	It("should block if exceeds max buffer size.", func() {
		ch := NewChanPeekable[int](1)
		runtime.Gosched()

		for i := 0; i < 2; i++ {
			select {
			case ch.In() <- i:
			default:
				Expect(i).To(Equal(1))
			}
		}
	})

	It("should peek return head value.", func() {
		ch := NewChanPeekable[int](1)
		runtime.Gosched()

		ch.In() <- 2
		runtime.Gosched()
		Expect(ch.Len()).To(Equal(1))
		Expect(ch.Peek()).To(Equal(2))
	})

	It("should peek return empty value when channel empty.", func() {
		ch := NewChanPeekable[int](1)
		runtime.Gosched()

		Expect(ch.Len()).To(Equal(0))
		Expect(ch.Peek()).To(Equal(0))

		ch.In() <- 1
		out := <-ch.Out()
		Expect(out).To(Equal(1))
		Expect(ch.Len()).To(Equal(0))
		Expect(ch.Peek()).To(Equal(0))
	})

	It("should insert item be read out after closed.", func() {
		ch := NewChanPeekable[int](1)
		runtime.Gosched()

		ch.In() <- 1
		ch.Close()

		out := <-ch.Out()
		Expect(out).To(Equal(1))
		Expect(ch.Len()).To(Equal(0))

		Expect(<-ch.Out()).To(Equal(0))
	})
})
