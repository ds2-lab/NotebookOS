package scheduling

const (
	DefaultSchedulingPolicy PolicyName = "default"
	Static                  PolicyName = "static"
	DynamicV3               PolicyName = "dynamic-v3"
	DynamicV4               PolicyName = "dynamic-v4"
	FcfsBatch               PolicyName = "fcfs-batch"

	// BindResourcesAtTrainingStart indicates that resources are to be committed when training begins and
	// uncommitted when training ends.
	BindResourcesAtTrainingStart ResourceBindingMode = "BindResourcesAtTrainingStart"
	// BindResourcesWhenContainerScheduled indicates that resources are to be committed when a container is
	// scheduled and only uncommitted when that container is evicted.
	BindResourcesWhenContainerScheduled ResourceBindingMode = "BindResourcesWhenContainerScheduled"

	// SingleTrainingEvent indicates that a KernelContainer exists for the duration of a single training event
	// before being terminated and reclaimed.
	SingleTrainingEvent ContainerLifetime = "SingleTrainingEvent"
	// LongRunning indicates that a KernelContainer exists for an extended period of time, beyond the scope of a single
	// training event. LongRunning KernelContainer instances are not reclaimed until they are migrated or the associated
	// UserSession is terminated by the user.
	LongRunning ContainerLifetime = "LongRunning"
)

// PolicyName indicates the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
type PolicyName string

// ResourceBindingMode indicates the time at which resources are (exclusively) committed to containers, and implicitly
// when they are uncommitted from containers as well.
type ResourceBindingMode string

// ContainerLifetime defines how long containers of a kernel live. Options include for the duration of a single
// training event or long-running.
type ContainerLifetime string

// Policy defines a high-level scheduling policy.
//
// Scheduling policies encapsulate configuration parameters that are common to multiple/all scheduling policies.
type Policy interface {
	// PolicyName returns the PolicyName of the target scheduling Policy.
	PolicyName() PolicyName

	// NumReplicas returns the number of replicas that each kernel should have under the target scheduling Policy.
	NumReplicas() int

	// ResourceBindingMode returns the ResourceBindingMode of the target scheduling Policy.
	ResourceBindingMode() ResourceBindingMode

	// ContainerLifetime returns the ContainerLifetime of KernelContainer instances created under the target Policy.
	ContainerLifetime() ContainerLifetime

	// PostExecutionStatePolicy returns the PostExecutionStatePolicy of the target scheduling Policy.
	//
	// A PostExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code.
	PostExecutionStatePolicy() PostExecutionStatePolicy
}

// PostExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code.
//
// The main properties here are (a) whether the kernel should perform a (simulated) network write, and (b) if so, then
// whether that (simulated) network write should occur on the critical path or in the background.
type PostExecutionStatePolicy interface {
	// ShouldPerformOperation returns a bool flag indicating whether the kernel should perform a (simulated)
	// network write operation after a successful code execution.
	ShouldPerformOperation() bool

	// OperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
	// performed after a successful code execution should be on the critical path.
	//
	// If the ShouldPerformOperation method of the target PostExecutionStatePolicy returns false, then
	// the OperationIsOnCriticalPath method will also return false.
	OperationIsOnCriticalPath() bool
}

// PreExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code.
//
// The main properties here are (a) whether the kernel should perform a (simulated) network write, and (b) if so, then
// whether that (simulated) network write should occur on the critical path or in the background.
type PreExecutionStatePolicy interface {
	// ShouldPerformOperation returns a bool flag indicating whether the kernel should perform a (simulated)
	// network read operation before executing user-submitted code.
	//
	// Such a read operation would be to retrieve the current or latest model state/parameters and any required
	// training data.
	ShouldPerformOperation() bool

	// OperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
	// performed before executing user-submitted code should be on the critical path.
	//
	// If the ShouldPerformOperation method of the target PostExecutionStatePolicy returns false, then
	// the OperationIsOnCriticalPath method will also return false.
	OperationIsOnCriticalPath() bool
}
