package transaction_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/shopspring/decimal"
)

var _ = Describe("Resources", func() {
	It("Will correctly initialize a resource", func() {
		res := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
		Expect(res).ToNot(BeNil())

		working := types.ToDecimalSpec(res.Working())
		Expect(working).ToNot(BeNil())

		Expect(res.IsMutable()).To(BeFalse())

		Expect(utils.EqualWithTolerance(working.Millicpus, decimal.NewFromFloat(5.0)))
		Expect(utils.EqualWithTolerance(working.MemoryMb, decimal.NewFromFloat(10.0)))
		Expect(utils.EqualWithTolerance(working.GPUs, decimal.NewFromFloat(15.0)))
		Expect(utils.EqualWithTolerance(working.VRam, decimal.NewFromFloat(20.0)))

		mutableRes := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), true)
		Expect(mutableRes).ToNot(BeNil())
		Expect(mutableRes.IsMutable()).To(BeTrue())
	})

	Context("Comparing resources ('greater than' or 'less than or equal')", func() {
		Context("Less than or equal", func() {
			It("Will return true when resource a is equal to resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind := res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeTrue())
				Expect(offendingKind).To(Equal(transaction.NoResource))
			})

			It("Will return false when resource a > resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(0, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind := res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.CPU))

				res2 = transaction.NewResources(types.NewDecimalSpec(10, 0, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind = res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.Memory))

				res2 = transaction.NewResources(types.NewDecimalSpec(10, 15, 0, 20), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind = res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.GPU))

				res2 = transaction.NewResources(types.NewDecimalSpec(10, 10, 20, 0), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind = res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.VRAM))
			})

			It("Will return true when resource a < resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(0, 0, 0, 0), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				lessThanOrEqual, offendingKind := res1.LessThanOrEqual(res2.Working())
				Expect(lessThanOrEqual).To(BeTrue())
				Expect(offendingKind).To(Equal(transaction.NoResource))
			})
		})

		Context("Greater than", func() {
			It("Will return true when resource a > resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(0, 0, 0, 0), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind := res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeTrue())
				Expect(offendingKind).To(Equal(transaction.NoResource))
			})

			It("Will return false when resource a is equal to resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind := res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.CPU))

				res2 = transaction.NewResources(types.NewDecimalSpec(0, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.Memory))

				res2 = transaction.NewResources(types.NewDecimalSpec(0, 0, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.GPU))

				res2 = transaction.NewResources(types.NewDecimalSpec(0, 0, 0, 20), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.VRAM))
			})

			It("Will return false when resource a < resource b", func() {
				res1 := transaction.NewResources(types.NewDecimalSpec(0, 10, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				res2 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
				Expect(res2).ToNot(BeNil())

				greaterThan, offendingKind := res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.CPU))

				res1 = transaction.NewResources(types.NewDecimalSpec(10, 0, 15, 20), false)
				Expect(res1).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.Memory))

				res1 = transaction.NewResources(types.NewDecimalSpec(10, 15, 0, 20), false)
				Expect(res1).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.GPU))

				res1 = transaction.NewResources(types.NewDecimalSpec(10, 15, 20, 0), false)
				Expect(res1).ToNot(BeNil())

				greaterThan, offendingKind = res1.GreaterThan(res2.Working())
				Expect(greaterThan).To(BeFalse())
				Expect(offendingKind).To(Equal(transaction.VRAM))
			})
		})
	})

	Context("Testing resource equality", func() {
		It("Will correctly evaluate the equality of two equal resource", func() {
			res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
			Expect(res1).ToNot(BeNil())

			res2 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
			Expect(res2).ToNot(BeNil())

			Expect(res1.Equals(res2)).To(BeTrue())
		})
		It("Will correctly evaluate the equality of a resource and an equal spec", func() {
			res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
			Expect(res1).ToNot(BeNil())

			spec := types.NewDecimalSpec(5, 10, 15, 20)
			Expect(spec).ToNot(BeNil())

			Expect(res1.Equals(spec)).To(BeTrue())
		})

		It("Will correctly evaluate the equality of two unequal resource", func() {
			res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
			Expect(res1).ToNot(BeNil())

			res2 := transaction.NewResources(types.NewDecimalSpec(20, 15, 10, 5), false)
			Expect(res2).ToNot(BeNil())

			Expect(res1.Equals(res2)).To(BeFalse())
		})

		It("Will correctly evaluate the equality of a resource and an unequal spec", func() {
			res1 := transaction.NewResources(types.NewDecimalSpec(5, 10, 15, 20), false)
			Expect(res1).ToNot(BeNil())

			spec := types.ToDecimalSpec(types.NewDecimalSpec(20, 15, 10, 5))
			Expect(spec).ToNot(BeNil())

			Expect(res1.Equals(spec)).To(BeFalse())
		})
	})

	It("Will disallow the modification of immutable resources", func() {
		res := transaction.NewResources(types.NewDecimalSpec(0, 0, 0, 0), false)
		Expect(res).ToNot(BeNil())

		addToImmutableResource := func() {
			res.Add(types.NewDecimalSpec(10, 10, 10, 10))
		}

		subtractFromImmutableResource := func() {
			res.Add(types.NewDecimalSpec(10, 10, 10, 10))
		}

		Expect(addToImmutableResource).To(Panic())
		Expect(subtractFromImmutableResource).To(Panic())
	})

	It("Will correctly modify mutable resources", func() {
		res := transaction.NewResources(types.NewDecimalSpec(0, 0, 0, 0), true)
		Expect(res).ToNot(BeNil())

		res.Add(types.NewDecimalSpec(10, 10, 10, 10))

		working := res.Working()
		Expect(working).ToNot(BeNil())
	})
})
