package utils

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AvailablePorts", func() {
	It("should allocate ports as expected", func() {
		ports := NewAvailablePorts(9000, 20, 4)

		Expect(ports.TotalNumPorts()).To((Equal(20)))
		Expect(ports.NumPortsAvailable()).To((Equal(20)))
		Expect(ports.AllocationSize()).To(Equal(4))

		alloc, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc)).To(Equal(4))
		Expect(alloc).To(Equal([]int{9000, 9001, 9002, 9003}))

		alloc, err = ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc)).To(Equal(4))
		Expect(alloc).To(Equal([]int{9004, 9005, 9006, 9007}))

		alloc, err = ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc)).To(Equal(4))
		Expect(alloc).To(Equal([]int{9008, 9009, 9010, 9011}))

		alloc, err = ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc)).To(Equal(4))
		Expect(alloc).To(Equal([]int{9012, 9013, 9014, 9015}))

		alloc, err = ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc)).To(Equal(4))
		Expect(alloc).To(Equal([]int{9016, 9017, 9018, 9019}))

		alloc, err = ports.RequestPorts()
		Expect(err).To(Equal(ErrInsufficientPortsAvailable))
		Expect(len(alloc)).To(Equal(0))
	})

	It("should allow ports to be returned and reallocated", func() {
		ports := NewAvailablePorts(9000, 4, 4)

		Expect(ports.TotalNumPorts()).To((Equal(4)))
		Expect(ports.NumPortsAvailable()).To((Equal(4)))
		Expect(ports.AllocationSize()).To(Equal(4))

		alloc1, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc1)).To(Equal(4))
		Expect(alloc1).To(Equal([]int{9000, 9001, 9002, 9003}))

		alloc2, err := ports.RequestPorts()
		Expect(err).To(Equal(ErrInsufficientPortsAvailable))
		Expect(len(alloc2)).To(Equal(0))

		err = ports.ReturnPorts(alloc1)
		Expect(err).To(BeNil())

		alloc3, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc3)).To(Equal(4))
		Expect(alloc3).To(Equal([]int{9000, 9001, 9002, 9003}))
	})

	It("should allow ports to be returned and reallocated in order", func() {
		ports := NewAvailablePorts(9000, 4, 2)

		Expect(ports.TotalNumPorts()).To((Equal(4)))
		Expect(ports.NumPortsAvailable()).To((Equal(4)))
		Expect(ports.AllocationSize()).To(Equal(2))

		alloc1, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc1)).To(Equal(2))
		Expect(alloc1).To(Equal([]int{9000, 9001}))

		alloc2, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc2)).To(Equal(2))
		Expect(alloc2).To(Equal([]int{9002, 9003}))

		alloc3, err := ports.RequestPorts()
		Expect(err).To(Equal(ErrInsufficientPortsAvailable))
		Expect(len(alloc3)).To(Equal(0))

		err = ports.ReturnPorts(alloc1)
		Expect(err).To(BeNil())

		alloc4, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc4)).To(Equal(2))
		Expect(alloc4).To(Equal([]int{9000, 9001}))

		err = ports.ReturnPorts(alloc4)
		Expect(err).To(BeNil())

		err = ports.ReturnPorts(alloc2)
		Expect(err).To(BeNil())

		alloc5, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc5)).To(Equal(2))
		Expect(alloc5).To(Equal([]int{9000, 9001}))

		alloc6, err := ports.RequestPorts()
		Expect(err).To(BeNil())
		Expect(len(alloc6)).To(Equal(2))
		Expect(alloc6).To(Equal([]int{9002, 9003}))
	})

	It("should panic if already-available ports are returned", func() {
		ports := NewAvailablePorts(9000, 4, 4)

		Expect(func() {
			ports.ReturnPorts([]int{9000, 9001, 9002, 9003})
		}).To(Panic())
	})

	It("should panic for invalid arguments upon creation", func() {
		// Invalid starting port (negative).
		Expect(func() {
			_ = NewAvailablePorts(-1, 4, 4)
		}).To(Panic())

		// Invalid starting port (zero).
		Expect(func() {
			_ = NewAvailablePorts(0, 4, 4)
		}).To(Panic())

		// Invalid starting port (reserved).
		Expect(func() {
			_ = NewAvailablePorts(1023, 4, 4)
		}).To(Panic())

		// Invalid port range (too large).
		Expect(func() {
			_ = NewAvailablePorts(65535, 4, 4)
		}).To(Panic())

		// Invalid number of ports.
		Expect(func() {
			_ = NewAvailablePorts(9000, 0, 4)
		}).To(Panic())

		// Invalid allocation size.
		Expect(func() {
			_ = NewAvailablePorts(9000, 4, 0)
		}).To(Panic())
	})
})
