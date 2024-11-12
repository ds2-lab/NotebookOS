package types_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/zhangjyr/distributed-notebook/common/types"
)

var _ = Describe("Spec", func() {
	It("Will correctly add two DecimalSpec structs", func() {
		kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

		kernel2Spec := types.NewDecimalSpec(3500, 7500, 4, 16)

		kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

		Expect(kernel1And2Spec).ToNot(BeNil())

		Expect(kernel1And2Spec.GPU()).To(Equal(6.0))
		Expect(kernel1And2Spec.VRAM()).To(Equal(24.0))
		Expect(kernel1And2Spec.CPU()).To(Equal(7500.0))
		Expect(kernel1And2Spec.MemoryMB()).To(Equal(23500.0))
	})

	It("Will correctly subtract two DecimalSpec structs", func() {
		kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

		kernel2Spec := types.NewDecimalSpec(13500, 17500, 4, 16)

		diffSpec := kernel2Spec.Subtract(kernel1Spec)

		Expect(diffSpec).ToNot(BeNil())

		Expect(diffSpec.CPU()).To(Equal(9500.0))
		Expect(diffSpec.MemoryMB()).To(Equal(1500.0))
		Expect(diffSpec.GPU()).To(Equal(2.0))
		Expect(diffSpec.VRAM()).To(Equal(8.0))
	})

	It("Will correctly recognize a zero DecimalSpec", func() {
		zeroSpec := types.NewDecimalSpec(0, 0, 0, 0)

		Expect(zeroSpec).ToNot(BeNil())
		Expect(zeroSpec.IsZero()).To(BeTrue())
	})

	It("Will correctly evaluate two equal DecimalSpec structs", func() {
		spec1 := types.NewDecimalSpec(14500, 32500, 3, 13)

		spec2 := types.NewDecimalSpec(14500, 32500, 3, 13)

		Expect(spec1).ToNot(BeNil())
		Expect(spec2).ToNot(BeNil())

		Expect(spec1.Equals(spec2)).To(BeTrue())

		Expect(spec1.Equals(spec1)).To(BeTrue())
		Expect(spec2.Equals(spec2)).To(BeTrue())
	})

	It("Will correctly return an equal DecimalSpec via ToDecimalSpec", func() {
		spec1 := types.NewDecimalSpec(14500, 32500, 3, 13)

		spec2 := types.ToDecimalSpec(spec1)

		Expect(spec1).ToNot(BeNil())
		Expect(spec2).ToNot(BeNil())

		Expect(spec1.Equals(spec2)).To(BeTrue())

		spec3 := types.ToDecimalSpec(spec2)

		Expect(spec1.Equals(spec3)).To(BeTrue())
		Expect(spec2.Equals(spec3)).To(BeTrue())
	})

	It("Will correctly convert a float-based Spec to an equal DecimalSpec", func() {
		floatSpec := &types.Float64Spec{
			GPUs:      4.0,
			VRam:      12.0,
			Millicpus: 2000.0,
			MemoryMb:  13500.0,
		}

		decimalSpec := types.NewDecimalSpec(2000, 13500, 4, 12)

		convertedDecimalSpec := types.ToDecimalSpec(floatSpec)

		Expect(floatSpec).ToNot(BeNil())
		Expect(convertedDecimalSpec).ToNot(BeNil())
		Expect(decimalSpec).ToNot(BeNil())

		Expect(decimalSpec.Equals(convertedDecimalSpec)).To(BeTrue())

		Expect(decimalSpec.Equals(floatSpec)).To(BeTrue())
	})

	It("Will correctly update the DecimalSpec", func() {
		spec := types.NewDecimalSpec(14500, 32500, 3, 13)

		spec.UpdateSpecGPUs(4)
		Expect(spec.GPU()).To(Equal(4.0))

		spec.UpdateSpecCPUs(12000)
		Expect(spec.CPU()).To(Equal(12000.0))

		spec.UpdateSpecMemoryMB(27480)
		Expect(spec.MemoryMB()).To(Equal(27480.0))
	})

	It("Will correctly clone DecimalSpecs", func() {
		spec := types.NewDecimalSpec(14500, 32500, 3, 13)
		Expect(spec).ToNot(BeNil())

		spec1 := spec.Clone()
		Expect(spec1).ToNot(BeNil())
		Expect(spec.Equals(spec1)).To(BeTrue())

		spec2 := spec.CloneDecimalSpec()
		Expect(spec2).ToNot(BeNil())
		Expect(spec.Equals(spec2)).To(BeTrue())

		Expect(spec1.(*types.DecimalSpec).Equals(spec2)).To(BeTrue())
	})

	It("Will correctly validate two equal DecimalSpec instances", func() {
		spec1 := types.NewDecimalSpec(14500, 32500, 3, 13)
		spec2 := types.NewDecimalSpec(14500, 32500, 3, 13)

		Expect(spec1.Validate(spec2)).To(BeTrue())
	})

	It("Will correctly validate two unequal DecimalSpec instances", func() {
		kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

		kernel2Spec := types.NewDecimalSpec(13500, 17500, 4, 16)

		Expect(kernel2Spec.Validate(kernel1Spec)).To(BeTrue())
		Expect(kernel1Spec.Validate(kernel2Spec)).To(BeFalse())
	})
})
