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
})
