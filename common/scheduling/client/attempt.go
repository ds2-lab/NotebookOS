package client

import (
	"errors"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"go.uber.org/atomic"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"time"
)

const (
	maxSemaphoreWeight int64 = 999999999
)

var (
	ErrFailureUnspecified = errors.New("the kernel replica container creation operation failed for an unspecified reason")
	ErrAlreadyComplete    = errors.New("the kernel replica container creation operation is already marked as having completed")
)

// kernel is a wrapper around the scheduling.Kernel interface with an extra method concludeSchedulingReplicaContainers.
// The concludeSchedulingReplicaContainers method is required by the CreateReplicaContainersAttempt struct.
type kernel interface {
	scheduling.Kernel

	// concludeSchedulingReplicaContainers is called automatically by a CreateReplicaContainersAttempt struct when
	// the container creation operation associated with the CreateReplicaContainersAttempt concludes.
	//
	// concludeSchedulingReplicaContainers should return true, meaning that the kernel's flag that indicates whether
	// an active container-creation operation is occurring was successfully flipped from 1 --> 0.
	//
	// If concludeSchedulingReplicaContainers returns false, then the CreateReplicaContainersAttempt struct that is
	// invoking the concludeSchedulingReplicaContainers method will ultimately end up panicking.
	concludeSchedulingReplicaContainers() bool
}

// CreateReplicaContainersAttempt is similar to kernelDescheduleAttempt, but CreateReplicaContainersAttempt is used
// to keep track of a kernel whose kernel replicas and kernel containers are being created, rather than removed.
type CreateReplicaContainersAttempt struct {
	// Semaphore is used to enable waiting with a timeout for the container creation operation(s) to complete.
	Semaphore *semaphore.Weighted

	// startedAt is the time at which the container creation operation(s) began.
	startedAt time.Time

	// kernelId is the ID of the associated scheduling.Kernel whose kernel replica(s) and kernel container(s)
	// is/are being created.
	kernelId string

	// complete indicates whether the container creation operation(s) has/have completed.
	complete atomic.Bool

	// Succeeded indicates whether the container creation operation(s) succeeded.
	succeeded atomic.Bool

	// FailureReason is set to a value if there is an error associated with the failure of the
	// container creation operation(s) succeeded.
	failureReason error

	// kernel is the target of the associated container creation operation(s).
	kernel kernel

	// placementInProgress indicates whether the process of placing and creating the scheduling.KernelContainer
	// instances has started. Generally, if this stage is reached, then the operation will most-likely complete
	// successfully, as errors are unlikely, and it means that resources were available and whatnot.
	placementInProgress atomic.Bool
}

// newCreateReplicaContainersAttempt creates a new CreateReplicaContainersAttempt struct and returns a pointer to it.
func newCreateReplicaContainersAttempt(kernel kernel) *CreateReplicaContainersAttempt {
	weightedSemaphore := semaphore.NewWeighted(maxSemaphoreWeight)

	// Acquire the weightedSemaphore so anybody who calls Wait will have to wait.
	err := weightedSemaphore.Acquire(context.Background(), 999999)
	if err != nil {
		panic(err)
	}

	return &CreateReplicaContainersAttempt{
		Semaphore: weightedSemaphore,
		startedAt: time.Now(),
		kernelId:  kernel.ID(),
		kernel:    kernel,
	}
}

// KernelId returns the kernel ID of the scheduling.Kernel associated with the target CreateReplicaContainersAttempt.
func (a *CreateReplicaContainersAttempt) KernelId() string {
	return a.kernelId
}

// StartedAt returns the time at which the target CreateReplicaContainersAttempt began.
func (a *CreateReplicaContainersAttempt) StartedAt() time.Time {
	return a.startedAt
}

// TimeElapsed returns the amount of time that has elapsed since the target CreateReplicaContainersAttempt began.
func (a *CreateReplicaContainersAttempt) TimeElapsed() time.Duration {
	return time.Since(a.startedAt)
}

// PlacementInProgress returns true if the process of placing and creating the scheduling.KernelContainer
// instances has started. Generally, if this stage is reached, then the operation will most-likely complete
// successfully, as errors are unlikely, and it means that resources were available and whatnot.
func (a *CreateReplicaContainersAttempt) PlacementInProgress() bool {
	return a.placementInProgress.Load()
}

// Succeeded returns true if the container creation operation(s) succeeded.
func (a *CreateReplicaContainersAttempt) Succeeded() bool {
	return a.succeeded.Load()
}

// FailureReason returns a non-nil value if there is an error associated with the failure of the
// container creation operation(s) succeeded.
func (a *CreateReplicaContainersAttempt) FailureReason() error {
	return a.failureReason
}

// Kernel returns the scheduling.Kernel associated with the target CreateReplicaContainersAttempt (i.e., the
// scheduling.Kernel whose scheduling.KernelContainer instances are being created).
func (a *CreateReplicaContainersAttempt) Kernel() scheduling.Kernel {
	return a.kernel
}

// Wait blocks until the target CreateReplicaContainersAttempt is finished, or until the given
// context.Context is cancelled.
//
// If the operation completes in a failed state and there's a failure reason, then the failure reason will be returned.
func (a *CreateReplicaContainersAttempt) Wait(ctx context.Context) error {
	err := a.Semaphore.Acquire(ctx, 1)
	if err != nil {
		return err
	}

	if a.succeeded.Load() {
		return nil
	}

	if a.failureReason != nil {
		return a.failureReason
	}

	if !a.complete.Load() {
		panic("Expected CreateReplicaContainersAttempt to be complete.")
	}

	// This probably shouldn't happen... If it failed, then a reason should have been provided.
	return ErrFailureUnspecified
}

// IsComplete returns true if the target CreateReplicaContainersAttempt has finished.
//
// Note that if IsComplete is true, that doesn't necessarily mean that the associated container creation operation
// finished successfully. It may have encountered errors or timed-out on its own.
func (a *CreateReplicaContainersAttempt) IsComplete() bool {
	return a.complete.Load()
}

// SetDone records that the target CreateReplicaContainersAttempt has finished.
//
// If the operation failed, then the reason, in the form of an error, should be passed to SetDone.
//
// If the target CreateReplicaContainersAttempt has already been marked as having completed, then SetDone will panic.
func (a *CreateReplicaContainersAttempt) SetDone(failureReason error) {
	if a.complete.Load() {
		panic(ErrAlreadyComplete)
	}

	// Release maxSemaphoreWeight so that Wait() can be called an arbitrary number of times.
	defer a.Semaphore.Release(maxSemaphoreWeight)

	a.complete.Store(true)
	a.failureReason = failureReason
	a.succeeded.Store(failureReason == nil)

	if !a.kernel.concludeSchedulingReplicaContainers() {
		panic("Failed to conclude kernel replica container creation operation")
	}
}
