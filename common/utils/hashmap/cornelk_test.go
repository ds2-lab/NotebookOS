package hashmap_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
)

var _ = Describe("Cornel-K Map Tests", func() {
	var m *hashmap.CornelkMap[string, int]

	BeforeEach(func() {
		m = hashmap.NewCornelkMap[string, int](10)
	})

	It("should store and load values", func() {
		m.Store("key1", 42)
		value, ok := m.Load("key1")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(42))
	})

	It("should return false for non-existent keys", func() {
		_, ok := m.Load("key2")
		Expect(ok).To(BeFalse())
	})

	It("should delete a key", func() {
		m.Store("key3", 84)
		m.Delete("key3")
		_, ok := m.Load("key3")
		Expect(ok).To(BeFalse())
	})

	It("should load and delete a key", func() {
		m.Store("key4", 128)
		value, ok := m.LoadAndDelete("key4")
		Expect(ok).To(BeTrue())
		Expect(value).To(Equal(128))
		_, ok = m.Load("key4")
		Expect(ok).To(BeFalse())
	})

	It("should load or store a key", func() {
		value, loaded := m.LoadOrStore("key5", 256)
		Expect(loaded).To(BeFalse())
		Expect(value).To(Equal(256))

		value, loaded = m.LoadOrStore("key5", 512)
		Expect(loaded).To(BeTrue())
		Expect(value).To(Equal(256))
	})

	It("should compare and swap values", func() {
		m.Store("key6", 512)
		value, swapped := m.CompareAndSwap("key6", 512, 1024)
		Expect(swapped).To(BeTrue())
		Expect(value).To(Equal(1024))

		value, swapped = m.CompareAndSwap("key6", 512, 2048)
		Expect(swapped).To(BeFalse())
		Expect(value).To(Equal(512))
	})

	It("should iterate over elements with Range", func() {
		m.Store("key7", 10)
		m.Store("key8", 20)
		m.Store("key9", 30)

		elements := make(map[string]int)
		m.Range(func(k string, v int) bool {
			elements[k] = v
			return true
		})

		Expect(elements).To(HaveKeyWithValue("key7", 10))
		Expect(elements).To(HaveKeyWithValue("key8", 20))
		Expect(elements).To(HaveKeyWithValue("key9", 30))
	})

	It("should return the correct length", func() {
		m.Store("key10", 100)
		m.Store("key11", 200)
		Expect(m.Len()).To(Equal(2))
	})
})
