package scheduling

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ScaleInOperation indicates that the ScaleOperation will be decreasing the number of Host instances within the Cluster.
	ScaleInOperation ScaleOperationType = "ScaleInOperation"

	// ScaleOutOperation indicates that the ScaleOperation will be increasing the number of Host instances within the Cluster.
	ScaleOutOperation ScaleOperationType = "ScaleOutOperation"

	ScaleOperationAwaitingStart ScaleOperationStatus = "awaiting_start"
	ScaleOperationInProgress    ScaleOperationStatus = "in_progress"
	ScaleOperationComplete      ScaleOperationStatus = "complete"
	ScaleOperationErred         ScaleOperationStatus = "error"
)

var (
	ErrInvalidTargetScale      = status.Error(codes.InvalidArgument, "invalid target scale specified")
	ErrScalingInvalidOperation = status.Error(codes.Internal, "scale operation is in invalid state for requested operation")
	ErrIncorrectScaleOperation = status.Error(codes.Internal, "scale operation is of incorrect type")
)

type ScaleOperationType string

func (s ScaleOperationType) String() string {
	return string(s)
}

type ScaleOperationStatus string

func (s ScaleOperationStatus) String() string {
	return string(s)
}

// ScaleOperation encapsulates the bookkeeping required for adjusting the scale of the Cluster.
// As of right now, the actual business logic required for performing the scale operation (i.e., adding or removing
// nodes from the Cluster) is not implemented, referenced by, or contained within a ScaleOperation struct.
//
// Instead, the associated business logic is implemented directly within the ClusterGateway.
type ScaleOperation struct {
	OperationId      string               `json:"request_id"`
	InitialScale     int32                `json:"initial_scale"`
	TargetScale      int32                `json:"target_scale"`
	OperationType    ScaleOperationType   `json:"scale_operation_type"`
	RegistrationTime time.Time            `json:"registration_time"`
	StartTime        time.Time            `json:"start_time"`
	EndTime          time.Time            `json:"end_time"`
	Status           ScaleOperationStatus `json:"status"`
	Error            error                `json:"error"` // Error is the error that caused ScaleOperation to enter the ScaleOperationErred state/status.
	NotificationChan chan struct{}        `json:"-"`

	// cond exists so that goroutines can wait for the scale operation to complete.
	cond   *sync.Cond
	condMu sync.Mutex

	mu sync.Mutex
}

// NewScaleOperation creates a new ScaleOperation struct and returns a pointer to it.
//
// Specifically, a tuple is returned, where the first element is a pointer to a new ScaleOperation struct, and
// the second element is an error, if one occurred. If an error did occur, then the pointer to the ScaleOperation
// struct will presumably be a null pointer.
func NewScaleOperation(operationId string, initialScale int32, targetScale int32) (*ScaleOperation, error) {
	scaleOperation := &ScaleOperation{
		OperationId:      operationId,
		NotificationChan: make(chan struct{}, 1),
		InitialScale:     initialScale,
		TargetScale:      targetScale,
		RegistrationTime: time.Now(),
		Status:           ScaleOperationAwaitingStart,
	}

	scaleOperation.cond = sync.NewCond(&scaleOperation.condMu)

	if targetScale < initialScale {
		scaleOperation.OperationType = ScaleInOperation
	} else if targetScale > initialScale {
		scaleOperation.OperationType = ScaleOutOperation
	} else /* targetScale == initialScale */ {
		wrappedErr := fmt.Errorf("%w: current scale and initial scale are equal (%d)", ErrInvalidTargetScale, targetScale)
		return nil, status.Error(codes.InvalidArgument, wrappedErr.Error())
	}

	return scaleOperation, nil
}

// IsScaleOutOperation returns true if the ScaleOperation is of type ScaleOutOperation.
func (op *ScaleOperation) IsScaleOutOperation() bool {
	return op.OperationType == ScaleOutOperation
}

// IsScaleInOperation returns true if the ScaleOperation is of type ScaleInOperation.
func (op *ScaleOperation) IsScaleInOperation() bool {
	return op.OperationType == ScaleInOperation
}

// String returns a string representation of the target ScaleOperation struct that is suitable for printing/logging.
func (op *ScaleOperation) String() string {
	return fmt.Sprintf("%s[Initial: %d, Target: %d, State: %s, ID: %s]",
		op.OperationType, op.InitialScale, op.TargetScale, op.Status.String(), op.OperationId)
}

// IsComplete returns true if the ScaleOperation has either completed successfully or stopped due to an error.
//
// Note: this acquires the "main" mutex of the ScaleOperation.
func (op *ScaleOperation) IsComplete() bool {
	op.mu.Lock()
	defer op.mu.Unlock()

	return op.Status == ScaleOperationComplete || op.Status == ScaleOperationErred
}

// CompletedSuccessfully returns true if the ScaleOperation completed successfully.
//
// If the ScaleOperation exited with an error, then CompletedSuccessfully will return false.
func (op *ScaleOperation) CompletedSuccessfully() bool {
	op.mu.Lock()
	defer op.mu.Unlock()

	return op.Status == ScaleOperationComplete
}

// IsErred returns true if the ScaleOperation exited due to an error state.
func (op *ScaleOperation) IsErred() bool {
	return op.Status == ScaleOperationErred
}

// Wait blocks the caller until the ScaleOperation has either completed successfully or terminates due to an error.
// If the operation has already completed, then this just returns immediately.
//
// Note: this does NOT directly acquire the "main" mutex of the ScaleOperation.
// This calls IsComplete, which acquires said mutex.
func (op *ScaleOperation) Wait() {
	// Just to avoid locking unnecessarily, we can go ahead and check if the operation is complete here.
	if op.IsComplete() {
		return
	}

	op.cond.L.Lock()
	for !op.IsComplete() {
		op.cond.Wait()
	}
	op.cond.L.Unlock()
}

// Start records that the ScaleOperation has started.
//
// Note: this acquires the "main" mutex of the ScaleOperation.
func (op *ScaleOperation) Start() error {
	op.mu.Lock()
	defer op.mu.Unlock()

	if op.Status != ScaleOperationAwaitingStart {
		return fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	op.StartTime = time.Now()
	op.Status = ScaleOperationInProgress

	return nil
}

// SetOperationFinished records that the ScaleOperation has completed successfully.
// This transitions the ScaleOperation to the ScaleOperationComplete state/status.
//
// Note: this acquires the "main" mutex of the ScaleOperation.
func (op *ScaleOperation) SetOperationFinished() error {
	op.mu.Lock()
	defer op.mu.Unlock()

	if op.Status != ScaleOperationAwaitingStart && op.Status != ScaleOperationInProgress {
		return fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	op.EndTime = time.Now()
	op.Status = ScaleOperationComplete

	// Wake up anybody waiting for the operation to complete.
	// Note: it is allowed but not required for the caller to hold cond.L during the call.
	op.cond.Broadcast()

	return nil
}

// SetOperationErred transitions the ScaleOperation to the ScaleOperationErred state/status.
//
// If the ScaleOperation has already been designated as having completed successfully, then this will not
// transition the ScaleOperation to the ScaleOperationErred unless the override parameter is true.
func (op *ScaleOperation) SetOperationErred(err error, override bool) error {
	op.mu.Lock()
	defer op.mu.Unlock()

	// If we've already completed successfully and override is false, then we'll return an error.
	if op.Status == ScaleOperationComplete && !override {
		return fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	// If we're already in an error state, then we'll return an error.
	if op.Status == ScaleOperationErred {
		return fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	op.EndTime = time.Now()
	op.Status = ScaleOperationErred
	op.Error = err

	// Wake up anybody waiting for the operation to complete.
	// Note: it is allowed but not required for the caller to hold cond.L during the call.
	op.cond.Broadcast()

	return nil
}

// GetDuration returns the duration of the ScaleOperation, if the ScaleOperation has finished.
// If the ScaleOperation is not yet complete, then GetDuration returns an error, and the returned
// time.Duration is meaningless.
func (op *ScaleOperation) GetDuration() (time.Duration, error) {
	if !op.IsComplete() {
		return time.Duration(-1), fmt.Errorf("%w: the scale operation has not yet finished", ErrScalingInvalidOperation)
	}

	return op.EndTime.Sub(op.StartTime), nil
}
