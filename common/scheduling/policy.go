package scheduling

const (
	DefaultSchedulingPolicy PolicyKey = "default"
	Static                  PolicyKey = "static"
	DynamicV3               PolicyKey = "dynamic-v3"
	DynamicV4               PolicyKey = "dynamic-v4"
	FcfsBatch               PolicyKey = "fcfs-batch"
	Reservation             PolicyKey = "reservation"

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

// PolicyKey indicates the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
type PolicyKey string

// ResourceBindingMode indicates the time at which resources are (exclusively) committed to containers, and implicitly
// when they are uncommitted from containers as well.
//
// The ResourceBindingMode of a scheduling Policy essentially indicates whether the Policy performs dynamic resource
// management/allocation or whether resources are instead statically bound to KernelContainer instances for the
// lifetime of the associated UserSession.
type ResourceBindingMode string

// ContainerLifetime defines how long containers of a kernel live. Options include for the duration of a single
// training event or long-running.
type ContainerLifetime string

// Policy defines a high-level scheduling policy.
//
// Scheduling policies encapsulate configuration parameters that are common to multiple/all scheduling policies.
type Policy interface {
	// PolicyKey returns the PolicyKey of the target scheduling Policy.
	//
	// A PolicyKey is a unique identifier that isn't necessarily meant to be human-readable (at least, the formatting
	// of the PolicyKey isn't necessarily supposed to look "nice").
	PolicyKey() PolicyKey

	// Name returns a human-readable, nicely-formatted name of the scheduling Policy suitable for logging, printing,
	// and/or displaying to users.
	Name() string

	// NumReplicas returns the number of replicas that each kernel should have under the target scheduling Policy.
	NumReplicas() int

	// ResourceBindingMode returns the ResourceBindingMode of the target scheduling Policy.
	//
	// The ResourceBindingMode of a scheduling Policy essentially indicates whether the Policy performs dynamic
	// resource management/allocation or whether resources are instead statically bound to KernelContainer instances
	// for the lifetime of the associated UserSession.
	ResourceBindingMode() ResourceBindingMode

	// ContainerLifetime returns the ContainerLifetime of KernelContainer instances created under the target Policy.
	ContainerLifetime() ContainerLifetime

	// PostExecutionStatePolicy returns the PostExecutionStatePolicy of the target scheduling Policy.
	//
	// A PostExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code with
	// respect to performing (or not performing) a (simulated) network write operation to checkpoint the latest
	// model state/parameters.
	PostExecutionStatePolicy() PostExecutionStatePolicy

	// PreExecutionStatePolicy returns the PreExecutionStatePolicy of the target scheduling Policy.
	//
	// A PreExecutionStatePolicy defines the behavior of a kernel after before executing user-submitted code with
	// respect to performing (or not performing) a (simulated) network read operation to retrieve the latest model
	// state/parameters and any required training data.
	PreExecutionStatePolicy() PreExecutionStatePolicy

	// ResourceScalingPolicy returns the ResourceScalingPolicy of the target scheduling Policy.
	ResourceScalingPolicy() ResourceScalingPolicy
}

// ResourceScalingPolicy defines the configuration of resource scaling (i.e., adding and/or removing Host instances ),
// as well as the configuration parameters that tune the scaling behavior.
type ResourceScalingPolicy interface {
	// AutoscalingPolicy returns the AutoscalingPolicy of the target scheduling Policy.
	AutoscalingPolicy() AutoscalingPolicy

	// ManualScalingPolicy returns the ManualScalingPolicy of the target scheduling Policy.
	ManualScalingPolicy() ManualScalingPolicy
}

// AutoscalingPolicy defines the auto-scaling configuration (i.e., automatically adding or removing Host instances
// to/from the Cluster).
type AutoscalingPolicy interface {
	// AutomaticScalingOutEnabled returns a bool indicating whether the Cluster can automatically add additional Host instances.
	AutomaticScalingOutEnabled() bool

	// AutomaticScalingInEnabled returns a flag indicating whether the Cluster can automatically remove Host instances.
	AutomaticScalingInEnabled() bool
}

// ManualScalingPolicy defines the configuration of manually-triggered scaling (i.e., manually adding or removing
// Host instances to/from the Cluster).
type ManualScalingPolicy interface {
	// ManualScalingOutEnabled returns a bool indicating whether the Cluster can add additional Host instances if
	// manually/explicitly instructed to do so.
	ManualScalingOutEnabled() bool

	// ManualScalingInEnabled returns a bool indicating whether the Cluster can reove Host instances if manually/explicitly
	// instructed to do so.
	ManualScalingInEnabled() bool
}

// PostExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code.
//
// The main properties here are (a) whether the kernel should perform a (simulated) network write, and (b) if so, then
// whether that (simulated) network write should occur on the critical path or in the background.
type PostExecutionStatePolicy interface {
	// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
	// network write operation after a successful code execution.
	ShouldPerformWriteOperation() bool

	// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
	// performed after a successful code execution should be on the critical path.
	//
	// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
	// the WriteOperationIsOnCriticalPath method will also return false.
	WriteOperationIsOnCriticalPath() bool
}

// PreExecutionStatePolicy defines the behavior of a kernel after completing an execution of user code.
//
// The main properties here are (a) whether the kernel should perform a (simulated) network write, and (b) if so, then
// whether that (simulated) network write should occur on the critical path or in the background.
type PreExecutionStatePolicy interface {
	// ShouldPerformReadOperation returns a bool flag indicating whether the kernel should perform a (simulated)
	// network read operation before executing user-submitted code.
	//
	// Such a read operation would be to retrieve the current or latest model state/parameters and any required
	// training data.
	ShouldPerformReadOperation() bool

	// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
	// performed before executing user-submitted code should be on the critical path.
	//
	// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
	// the ReadOperationIsOnCriticalPath method will also return false.
	ReadOperationIsOnCriticalPath() bool
}
