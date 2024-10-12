package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"log"
	"math"
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
	ErrTooManyNodesAffected    = status.Error(codes.Internal, "too many nodes have been added/removed during the scale operation")
	ErrClusterSizeMismatch     = status.Error(codes.Internal, "Cluster size and target scale are unequal despite scale operation being recorded as a success")
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

// ScaleOperationResult is an interface defining the result of a scale-in or scale-out operation.
type ScaleOperationResult interface {
	// GetPreviousNumNodes returns the number of Host instances within the Cluster
	// before the scale-in operation was performed.
	GetPreviousNumNodes() int32

	// GetCurrentNumNodes returns the number of Host instances within the Cluster
	// after the scale-in operation completed.
	GetCurrentNumNodes() int32

	// NumNodesAffected returns the number of Host instances that were created or terminated (depending
	//	// on whether a scale-out or a scale-in operation was performed, respectively).
	NumNodesAffected() int32

	// Nodes returns the IDs of each of the Host instances that was created or terminated (depending
	// on whether a scale-out or a scale-in operation was performed, respectively).
	Nodes() []string

	// Error returns the error (or errors that were joined together via errors.Join) that occurred while
	// performing the ScaleOperation, if any such error(s) did occur.
	Error() error

	// String returns the ScaleOperationResult formatted as a string that is suitable for logging.
	String() string
}

type BaseScaleOperationResult struct {
	// PreviousNumNodes is the number of Host instances within the Cluster
	// before the scale-in/scale-out operation was performed.
	PreviousNumNodes int32 `json:"prev_num_nodes"`

	// CurrentNumNodes is the number of Host instances within the Cluster
	// after the scale-in/scale-out operation completed.
	CurrentNumNodes int32 `json:"current_num_nodes"`

	// The error (or errors that were joined together via errors.Join) that occurred while
	// performing the ScaleOperation, if any such error(s) did occur.
	Err error `json:"error"`
}

// GetPreviousNumNodes returns the number of Host instances within the Cluster
// before the scale-in operation was performed.
func (s *BaseScaleOperationResult) GetPreviousNumNodes() int32 {
	return s.PreviousNumNodes
}

// GetCurrentNumNodes returns the number of Host instances within the Cluster
// after the scale-in operation completed.
func (s *BaseScaleOperationResult) GetCurrentNumNodes() int32 {
	return s.CurrentNumNodes
}

// String returns the BaseScaleOperationResult formatted as a string that is suitable for logging.
func (s *BaseScaleOperationResult) Error() error {
	return s.Err
}

// Error returns the error (or errors that were joined together via errors.Join) that occurred while
// performing the ScaleOperation, if any such error(s) did occur.
func (s *BaseScaleOperationResult) String() string {
	m, err := json.Marshal(s)
	if err != nil {
		panic(m)
	}

	return string(m)
}

// ScaleInOperationResult encapsulates the results of a scale-in operation.
type ScaleInOperationResult struct {
	*BaseScaleOperationResult

	// NumNodesTerminated is the number of Host instances that were terminated.
	NumNodesTerminated int32 `json:"num_nodes_terminated"`

	// NodesTerminated contains the IDs of each of the Host instances that was terminated.
	NodesTerminated []string `json:"nodes_terminated"`
}

func (s *ScaleInOperationResult) NumNodesAffected() int32 {
	return s.NumNodesTerminated
}

func (s *ScaleInOperationResult) Nodes() []string {
	return s.NodesTerminated
}

// String returns the ScaleInOperationResult formatted as a string that is suitable for logging.
func (s *ScaleInOperationResult) String() string {
	m, err := json.Marshal(s)
	if err != nil {
		panic(m)
	}

	return string(m)
}

// ScaleOutOperationResult encapsulates the results of a scale-in operation.
type ScaleOutOperationResult struct {
	*BaseScaleOperationResult

	// NumNodesCreated is the number of Host instances that were created.
	NumNodesCreated int32 `json:"num_nodes_created"`

	// NodesCreated contains the IDs of each of the Host instances that was created.
	NodesCreated []string `json:"nodes_created"`
}

func (s *ScaleOutOperationResult) NumNodesAffected() int32 {
	return s.NumNodesCreated
}

func (s *ScaleOutOperationResult) Nodes() []string {
	return s.NodesCreated
}

// String returns the ScaleOutOperationResult formatted as a string that is suitable for logging.
func (s *ScaleOutOperationResult) String() string {
	m, err := json.Marshal(s)
	if err != nil {
		panic(m)
	}

	return string(m)
}

// ScaleOperation encapsulates the bookkeeping required for adjusting the scale of the Cluster.
// As of right now, the actual business logic required for performing the scale operation (i.e., adding or removing
// nodes from the Cluster) is not implemented, referenced by, or contained within a ScaleOperation struct.
//
// Instead, the associated business logic is implemented directly within the ClusterGateway.
type ScaleOperation struct {
	OperationId              string               `json:"request_id"`
	InitialScale             int32                `json:"initial_scale"`
	TargetScale              int32                `json:"target_scale"`
	OperationType            ScaleOperationType   `json:"scale_operation_type"`
	RegistrationTime         time.Time            `json:"registration_time"`
	StartTime                time.Time            `json:"start_time"`
	EndTime                  time.Time            `json:"end_time"`
	Status                   ScaleOperationStatus `json:"status"`
	Error                    error                `json:"error"`                       // Error is the error that caused ScaleOperation to enter the ScaleOperationErred state/status.
	ExpectedNumAffectedNodes int                  `json:"expected_num_affected_nodes"` // ExpectedNumAffectedNodes is the expected number of Host instances to be added/removed.
	NodesAffected            []string             `json:"nodes_affected"`              // NodesAffected are the Host instances added/removed because of the ScaleOperation.
	Result                   ScaleOperationResult `json:"result"`
	NotificationChan         chan struct{}        `json:"-"`
	CoreLogicDoneChan        chan interface{}     `json:"-"`
	Cluster                  clusterInternal      `json:"-"`

	// cond exists so that goroutines can wait for the scale operation to complete.
	cond   *sync.Cond
	condMu sync.Mutex
	mu     sync.Mutex

	// This is what actually performs the scaling operation.
	// It is supplied by the Cluster implementation.
	executionFunc func()

	log logger.Logger
}

// getScaleOperationType returns the appropriate ScaleOperationType given the initial scale and target scale
// values for a particular ScaleOperation.
//
// This function will panic if initialScale and targetScale are equal.
//
// getScaleOperationType returns ScaleOutOperation if targetScale > initialScale and ScaleInOperation if
// targetScale < initialScale.
func getScaleOperationType(initialScale int32, targetScale int32) ScaleOperationType {
	if targetScale == initialScale {
		log.Fatalf("Invalid initial scale (%d) and target scale (%d) values specified.", initialScale, targetScale)
	}

	if initialScale > targetScale {
		return ScaleInOperation
	} else {
		return ScaleOutOperation
	}
}

// NewScaleInOperationWithTargetHosts creates a new ScaleOperation struct and returns a pointer to it.
// NewScaleInOperationWithTargetHosts differs from NewScaleOperation in that (a) it obviously creates a ScaleInOperation,
// rather than a ScaleOperation whose type is determined by the initialScale and targetScale arguments, and (b) allows
// the caller to explicitly specify the IDs of the Host instances that are to be removed from the Cluster.
//
// For now, the difference between the initial (current) scale and the target scale must equal the number of target
// Host IDs that are specified. That is, the caller cannot request a scale-in of (e.g.,) 10 hosts, while only explicitly
// specifying 5 Host IDs (with the idea that the Cluster would automatically select another 5 Host instances to remove).
func NewScaleInOperationWithTargetHosts(operationId string, initialScale int32, targetHosts []string, cluster clusterInternal) (*ScaleOperation, error) {
	// There should be at least one target Host specified.
	if len(targetHosts) == 0 {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("%w: current scale and initial scale are equal (no target hosts specified)", ErrInvalidTargetScale).Error())
	}

	// Ensure that the caller isn't trying to scale-in by too many hosts. There needs to be at least `NUM_REPLICAS`
	// hosts in the Cluster, where `NUM_REPLICAS` is the number of replicas of each Jupyter kernel.
	targetScale := initialScale - int32(len(targetHosts))
	if targetScale <= int32(cluster.NumReplicas()) {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("%w: too many target hosts specified (%d, with initial scale of %d); Cluster's minimum scale is %d", ErrInvalidTargetScale, targetScale, initialScale, cluster.NumReplicas()).Error())
	}

	// We're necessarily creating a scale-in operation here, so the target scale must be less than the initial scale.
	if targetScale > initialScale {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("%w: cannot create scale-in operation with initial scale of %d and target scale of %d", ErrInvalidTargetScale, initialScale, targetScale).Error())
	}

	// For now, the difference between the initial (current) scale and the target scale must equal the number of target
	// Host IDs that are specified. That is, the caller cannot request a scale-in of (e.g.,) 10 hosts, while only explicitly
	// specifying 5 Host IDs (with the idea that the Cluster would automatically select another 5 Host instances to remove).
	expectedNumAffectedNodes := int(math.Abs(float64(targetScale - initialScale)))
	if len(targetHosts) != expectedNumAffectedNodes {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Errorf("%w: specified %d target hosts, but difference between initial scale (%d) and target scale (%d) is %d",
				ErrInvalidTargetScale, len(targetHosts), initialScale, targetScale, expectedNumAffectedNodes).Error())
	}

	if cluster == nil {
		log.Fatalf("Cannot create new ScaleOperation when Cluster is nil...")
	}

	scaleOperation := &ScaleOperation{
		OperationId:              operationId,
		NotificationChan:         make(chan struct{}, 1),
		ExpectedNumAffectedNodes: expectedNumAffectedNodes,
		NodesAffected:            make([]string, 0, int(math.Abs(float64(targetScale-initialScale)))),
		InitialScale:             initialScale,
		TargetScale:              targetScale,
		RegistrationTime:         time.Now(),
		Status:                   ScaleOperationAwaitingStart,
		Cluster:                  cluster,
		OperationType:            getScaleOperationType(initialScale, targetScale),
		CoreLogicDoneChan:        make(chan interface{}),
	}

	scaleOperation.cond = sync.NewCond(&scaleOperation.condMu)

	executionFunc, err := cluster.getScaleInCommand(targetScale, targetHosts, scaleOperation.CoreLogicDoneChan)
	if err != nil {
		return nil, err
	}

	scaleOperation.executionFunc = executionFunc
	scaleOperation.log = config.GetLogger(
		fmt.Sprintf("%s-%s", scaleOperation.OperationType, scaleOperation.OperationId))
	return scaleOperation, nil
}

// NewScaleOperation creates a new ScaleOperation struct and returns a pointer to it.
//
// Specifically, a tuple is returned, where the first element is a pointer to a new ScaleOperation struct, and
// the second element is an error, if one occurred. If an error did occur, then the pointer to the ScaleOperation
// struct will presumably be a null pointer.
func NewScaleOperation(operationId string, initialScale int32, targetScale int32, cluster clusterInternal) (*ScaleOperation, error) {
	if targetScale == initialScale {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("%w: current scale and initial scale are equal (%d)", ErrInvalidTargetScale, targetScale).Error())
	}

	if cluster == nil {
		log.Fatalf("Cannot create new ScaleOperation when Cluster is nil...")
	}

	scaleOperation := &ScaleOperation{
		OperationId:              operationId,
		NotificationChan:         make(chan struct{}, 1),
		ExpectedNumAffectedNodes: int(math.Abs(float64(targetScale - initialScale))),
		NodesAffected:            make([]string, 0, int(math.Abs(float64(targetScale-initialScale)))),
		InitialScale:             initialScale,
		TargetScale:              targetScale,
		RegistrationTime:         time.Now(),
		Status:                   ScaleOperationAwaitingStart,
		Cluster:                  cluster,
		OperationType:            getScaleOperationType(initialScale, targetScale),
		CoreLogicDoneChan:        make(chan interface{}),
	}

	scaleOperation.cond = sync.NewCond(&scaleOperation.condMu)

	var (
		executionFunc func()
		err           error
	)
	if scaleOperation.OperationType == ScaleInOperation {
		executionFunc, err = cluster.getScaleInCommand(targetScale, []string{} /* No specific hosts targeted */, scaleOperation.CoreLogicDoneChan)
	} else {
		executionFunc = cluster.getScaleOutCommand(targetScale, scaleOperation.CoreLogicDoneChan)
	}

	if err != nil {
		return nil, err
	}

	scaleOperation.executionFunc = executionFunc
	scaleOperation.log = config.GetLogger(
		fmt.Sprintf("%s-%s ", scaleOperation.OperationType, scaleOperation.OperationId))
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

// execute performs the ScaleOperation.
func (op *ScaleOperation) execute(parentContext context.Context) (ScaleOperationResult, error) {
	if op.executionFunc == nil {
		log.Fatalf("Cannot execute ScaleOperation %s as its execution function is nil.", op.OperationId)
	}

	op.log.Debug("%s %s is beginning to execute.", op.OperationType, op.OperationId)

	timeoutInterval := time.Second * 30
	childContext, cancel := context.WithTimeout(parentContext, timeoutInterval)
	defer cancel()

	go op.executionFunc()

	select {
	case <-childContext.Done():
		{
			op.log.Error("Operation to adjust scale of virtual Docker nodes timed-out after %v.", timeoutInterval)
			if ctxErr := childContext.Err(); ctxErr != nil {
				op.log.Error("Additional error information regarding failed adjustment of virtual Docker nodes: %v", ctxErr)
				if transitionError := op.SetOperationErred(ctxErr, false); transitionError != nil {
					op.log.Error("Failed to transition to the erred state because: %v", transitionError)
				}

				return nil, status.Error(codes.Internal, ctxErr.Error())
			} else {
				if transitionError := op.SetOperationErred(fmt.Errorf("operation timed-out"), false); transitionError != nil {
					op.log.Error("Failed to transition to the erred state because: %v", transitionError)
				}

				return nil, status.Errorf(codes.Internal, "Operation to adjust scale of virtual Docker nodes timed-out after %v.", timeoutInterval)
			}
		}
	case notification := <-op.CoreLogicDoneChan: // Wait for the shell command above to finish.
		{
			if err, ok := notification.(error); ok {
				op.log.Error("Failed to adjust scale of virtual Docker nodes because: %v", err)
				op.Error = err
				if transitionError := op.SetOperationErred(err, false); transitionError != nil {
					op.log.Error("Failed to transition to the erred state because: %v", transitionError)
				}

				// If there was an error, then we'll return the error.
				return nil, status.Errorf(codes.Internal, err.Error())
			} else {
				op.log.Debug("%s %s has finished its core logic.", op.OperationType, op.OperationId)
				break
			}
		}
	}

	op.log.Debug("Waiting for new node(s) to connect or terminated nodes to be removed.")
	// The "core logic" of the operation has concluded insofar as we scaled-out or scaled-in the Cluster.
	// If we scaled-out, then we need to wait for the new Cluster Nodes to connect.
	// If we scaled-in, then we don't really have any waiting to do.
	// If it has already completed by the time we call Wait, then Wait just returns immediately.
	op.Wait()
	op.log.Debug("%s %s has finished (either in error or successfully).", op.OperationType, op.OperationId)

	if op.CompletedSuccessfully() {
		// The scale operation was recorded to have been a success.
		//
		// We still perform a sanity check here to make sure that the size of the Cluster is consistent with
		// the target scale of the scale operation.
		//
		// If they're unequal, then we'll return an error; however, it's possible that a node simply lost connection
		// right after the scale operation concluded. This does not necessarily indicate an internal error with
		// the logic of the Cluster Gateway or anything (but it might).
		currentSize := int32(op.Cluster.Len())
		if currentSize != op.TargetScale {
			op.log.Error("Current Cluster size (%d) does not match target scale (%d)...", currentSize, op.TargetScale)
			return nil, status.Error(codes.Internal, fmt.Errorf("%w: Cluster size = %d, target scale = %d",
				ErrClusterSizeMismatch, currentSize, op.TargetScale).Error())
		}

		timeElapsed, _ := op.GetDuration()
		op.log.Debug("Successfully adjusted number of virtual Docker nodes from %d to %d in %v.",
			op.InitialScale, op.TargetScale, timeElapsed)

		// Record the latency of the scale operation in/with Prometheus.
		var result ScaleOperationResult
		if op.IsScaleInOperation() {
			result = &ScaleInOperationResult{
				BaseScaleOperationResult: &BaseScaleOperationResult{
					PreviousNumNodes: op.InitialScale,
					CurrentNumNodes:  int32(op.Cluster.Len()),
				},
				NumNodesTerminated: int32(op.Cluster.Len()) - op.InitialScale,
				NodesTerminated:    op.NodesAffected,
			}

			op.Cluster.ClusterMetricsProvider().GetScaleInLatencyMillisecondsHistogram().Observe(float64(timeElapsed.Milliseconds()))
		} else {
			result = &ScaleOutOperationResult{
				BaseScaleOperationResult: &BaseScaleOperationResult{
					PreviousNumNodes: op.InitialScale,
					CurrentNumNodes:  int32(op.Cluster.Len()),
				},
				NumNodesCreated: int32(op.Cluster.Len()) - op.InitialScale,
				NodesCreated:    op.NodesAffected,
			}

			op.Cluster.ClusterMetricsProvider().GetScaleOutLatencyMillisecondsHistogram().Observe(float64(timeElapsed.Milliseconds()))
		}

		return result, nil
	} else {
		op.log.Error("Failed to adjust number of virtual Docker nodes from %d to %op.", op.InitialScale, op.TargetScale)

		if op.Error == nil {
			log.Fatalf("ScaleOperation \"%s\" is in '%s' state, but its Error field is nil...",
				op.OperationId, ScaleOperationErred.String())
		}

		return nil, status.Error(codes.Internal, op.Error.Error())
	}
}

// GetResult returns the result of the ScalingOperation, including any error that occurred.
func (op *ScaleOperation) GetResult() (ScaleOperationResult, error) {
	if !op.IsComplete() {
		return nil, ErrScalingInvalidOperation
	}

	if !op.CompletedSuccessfully() {
		return nil, op.Error
	}

	return op.Result, op.Error
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

// RegisterAffectedHost registers a Host as having been added or removed as a result of the ScaleOperation.
// RegisterAffectedHost determines whether the Host must have been added or removed based on the type of
// ScaleOperation that the target ScaleOperation is (i.e., scaling out vs. scaling in).
func (op *ScaleOperation) RegisterAffectedHost(hostId string) error {
	op.NodesAffected = append(op.NodesAffected, hostId)

	if len(op.NodesAffected) > op.ExpectedNumAffectedNodes {
		op.log.Error("Expected %d nodes to be affected; however, we just registered Affected Node #%d",
			op.ExpectedNumAffectedNodes, len(op.NodesAffected))

		return fmt.Errorf("%w: expected %d, but %d node(s) have/has been affected",
			ErrTooManyNodesAffected, op.ExpectedNumAffectedNodes, len(op.NodesAffected))
	} else {
		op.log.Debug("Affected host %d/%d has registered: host %s",
			len(op.NodesAffected), op.ExpectedNumAffectedNodes, hostId)
	}

	return nil
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

// Start begins the ScaleOperation.
//
// Note: this acquires the "main" mutex of the ScaleOperation.
func (op *ScaleOperation) Start(ctx context.Context) (ScaleOperationResult, error) {
	op.mu.Lock()

	if op.Status != ScaleOperationAwaitingStart {
		return nil, fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	op.StartTime = time.Now()
	op.Status = ScaleOperationInProgress
	op.mu.Unlock()

	return op.execute(ctx)
}

// SetOperationFinished records that the ScaleOperation has completed successfully.
// This transitions the ScaleOperation to the ScaleOperationComplete state/status.
//
// Note: this acquires the "main" mutex of the ScaleOperation.
func (op *ScaleOperation) SetOperationFinished() error {
	op.mu.Lock()

	if op.Status != ScaleOperationAwaitingStart && op.Status != ScaleOperationInProgress {
		return fmt.Errorf("%w: \"%s\"", ErrScalingInvalidOperation, op.Status)
	}

	op.EndTime = time.Now()
	op.Status = ScaleOperationComplete
	op.mu.Unlock()

	// Wake up anybody waiting for the operation to complete.
	// Note: it is allowed but not required for the caller to hold cond.L during the call.
	op.condMu.Lock()
	defer op.condMu.Unlock()
	op.cond.Broadcast()

	return nil
}

// SetOperationErred transitions the ScaleOperation to the ScaleOperationErred state/status.
//
// If the ScaleOperation has already been designated as having completed successfully, then this will not
// transition the ScaleOperation to the ScaleOperationErred unless the override parameter is true.
func (op *ScaleOperation) SetOperationErred(err error, override bool) error {
	op.mu.Lock()

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
	op.mu.Unlock()

	// Wake up anybody waiting for the operation to complete.
	// Note: it is allowed but not required for the caller to hold cond.L during the call.
	op.condMu.Lock()
	defer op.condMu.Unlock()
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
