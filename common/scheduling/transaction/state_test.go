package transaction_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
)

var _ = Describe("State", func() {
	Context("State validation", func() {
		It("Will correctly identify a valid state as being valid", func() {
			spec1 := types.NewDecimalSpec(50, 50, 50, 50)
			spec2 := types.NewDecimalSpec(100, 100, 100, 100)

			idle := transaction.NewResources(spec1, true)
			pending := transaction.NewResources(spec1, true)
			committed := transaction.NewResources(spec1, true)
			spec := transaction.NewResources(spec2, false)

			state := transaction.NewState(idle, pending, committed, spec)
			Expect(state).ToNot(BeNil())

			Expect(state.IdleResources().Equals(spec1)).To(BeTrue())
			Expect(state.PendingResources().Equals(spec1)).To(BeTrue())
			Expect(state.CommittedResources().Equals(spec1)).To(BeTrue())
			Expect(state.SpecResources().Equals(spec2)).To(BeTrue())

			err := state.Validate()
			if err != nil {
				GinkgoWriter.Printf("Validation error: %v\n", err)
			}

			Expect(err).To(BeNil())
		})

		Context("Invalid states", func() {
			It("Will correctly identify a state in which idle > spec as invalid", func() {
				spec1 := types.NewDecimalSpec(50, 50, 50, 50)
				spec2 := types.NewDecimalSpec(100, 100, 100, 100)
				spec3 := types.NewDecimalSpec(150, 150, 150, 150)

				idle := transaction.NewResources(spec3, true)
				pending := transaction.NewResources(spec1, true)
				committed := transaction.NewResources(spec1, true)
				spec := transaction.NewResources(spec2, false)

				state := transaction.NewState(idle, pending, committed, spec)

				err := state.Validate()
				if err != nil {
					GinkgoWriter.Printf("Validation error: %v\n", err)
				}

				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
			})

			It("Will correctly identify a state in which committed > spec as invalid", func() {
				spec1 := types.NewDecimalSpec(50, 50, 50, 50)
				spec2 := types.NewDecimalSpec(100, 100, 100, 100)
				spec3 := types.NewDecimalSpec(150, 150, 150, 150)

				idle := transaction.NewResources(spec1, true)
				pending := transaction.NewResources(spec1, true)
				committed := transaction.NewResources(spec3, true)
				spec := transaction.NewResources(spec2, false)

				state := transaction.NewState(idle, pending, committed, spec)

				err := state.Validate()
				if err != nil {
					GinkgoWriter.Printf("Validation error: %v\n", err)
				}

				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
			})

			It("Will correctly identify a state in which idle + committed != spec as invalid", func() {
				spec1 := types.NewDecimalSpec(50, 50, 50, 50)
				spec2 := types.NewDecimalSpec(100, 100, 100, 100)
				spec3 := types.NewDecimalSpec(25, 25, 25, 25)

				idle := transaction.NewResources(spec1, true)
				pending := transaction.NewResources(spec1, true)
				committed := transaction.NewResources(spec3, true)
				spec := transaction.NewResources(spec2, false)

				state := transaction.NewState(idle, pending, committed, spec)

				err := state.Validate()
				if err != nil {
					GinkgoWriter.Printf("Validation error: %v\n", err)
				}

				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
			})

			Context("Negative quantities", func() {
				specNegative := types.NewDecimalSpec(-25, 25, 25, 25)

				It("Will correctly identify a state in which idle has a negative field as invalid", func() {
					spec1 := types.NewDecimalSpec(50, 50, 50, 50)
					spec2 := types.NewDecimalSpec(100, 100, 100, 100)

					idle := transaction.NewResources(specNegative, true)
					pending := transaction.NewResources(spec1, true)
					committed := transaction.NewResources(spec1, true)
					spec := transaction.NewResources(spec2, false)

					state := transaction.NewState(idle, pending, committed, spec)

					err := state.Validate()
					if err != nil {
						GinkgoWriter.Printf("Validation error: %v\n", err)
					}

					Expect(err).ToNot(BeNil())
					Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
					Expect(errors.Is(err, transaction.ErrNegativeResourceCount)).To(BeTrue())
				})
				It("Will correctly identify a state in which pending has a negative field as invalid", func() {
					spec1 := types.NewDecimalSpec(50, 50, 50, 50)
					spec2 := types.NewDecimalSpec(100, 100, 100, 100)

					idle := transaction.NewResources(spec1, true)
					pending := transaction.NewResources(specNegative, true)
					committed := transaction.NewResources(spec1, true)
					spec := transaction.NewResources(spec2, false)

					state := transaction.NewState(idle, pending, committed, spec)

					err := state.Validate()
					if err != nil {
						GinkgoWriter.Printf("Validation error: %v\n", err)
					}

					Expect(err).ToNot(BeNil())
					Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
					Expect(errors.Is(err, transaction.ErrNegativeResourceCount)).To(BeTrue())
				})
				It("Will correctly identify a state in which committed has a negative field as invalid", func() {
					spec1 := types.NewDecimalSpec(50, 50, 50, 50)
					spec2 := types.NewDecimalSpec(100, 100, 100, 100)

					idle := transaction.NewResources(spec1, true)
					pending := transaction.NewResources(spec1, true)
					committed := transaction.NewResources(specNegative, true)
					spec := transaction.NewResources(spec2, false)

					state := transaction.NewState(idle, pending, committed, spec)

					err := state.Validate()
					if err != nil {
						GinkgoWriter.Printf("Validation error: %v\n", err)
					}

					Expect(err).ToNot(BeNil())
					Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
					Expect(errors.Is(err, transaction.ErrNegativeResourceCount)).To(BeTrue())
				})
				It("Will correctly identify a state in which spec has a negative field as invalid", func() {
					spec1 := types.NewDecimalSpec(50, 50, 50, 50)

					idle := transaction.NewResources(spec1, true)
					pending := transaction.NewResources(spec1, true)
					committed := transaction.NewResources(spec1, true)
					spec := transaction.NewResources(specNegative, false)

					state := transaction.NewState(idle, pending, committed, spec)

					err := state.Validate()
					if err != nil {
						GinkgoWriter.Printf("Validation error: %v\n", err)
					}

					Expect(err).ToNot(BeNil())
					Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
					Expect(errors.Is(err, transaction.ErrNegativeResourceCount)).To(BeTrue())
				})
			})
		})
	})
})
