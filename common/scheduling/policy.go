package scheduling

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/utils"
	"time"
)

const (
	DefaultSchedulingPolicy PolicyKey = "default"
	Static                  PolicyKey = "static"
	DynamicV3               PolicyKey = "dynamic-v3"
	DynamicV4               PolicyKey = "dynamic-v4"
	FcfsBatch               PolicyKey = "fcfs-batch"
	AutoScalingFcfsBatch    PolicyKey = "auto-scaling-fcfs-batch"
	Reservation             PolicyKey = "reservation"
	Gandiva                 PolicyKey = "gandiva"

	NoIdleSessionReclamation                IdleSessionReclamationPolicyKey = "none"
	GoogleColabIdleSessionReclamationPolicy IdleSessionReclamationPolicyKey = "google-colab"
	AdobeSenseiIdleSessionReclamationPolicy IdleSessionReclamationPolicyKey = "adobe-sensei"
	CustomIdleSessionReclamationPolicy      IdleSessionReclamationPolicyKey = "custom"

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

func (k PolicyKey) String() string {
	return string(k)
}

// IdleSessionReclamationPolicyKey indicates the IdleSessionReclamationPolicy that should be used.
type IdleSessionReclamationPolicyKey string

func (k IdleSessionReclamationPolicyKey) String() string {
	return string(k)
}

// ResourceBindingMode indicates the time at which resources are (exclusively) committed to containers, and implicitly
// when they are uncommitted from containers as well.
//
// The ResourceBindingMode of a scheduling Policy essentially indicates whether the Policy performs dynamic resource
// management/allocation or whether resources are instead statically bound to KernelContainer instances for the
// lifetime of the associated UserSession.
type ResourceBindingMode string

func (rbm ResourceBindingMode) String() string {
	return string(rbm)
}

// ContainerLifetime defines how long containers of a kernel live. Options include for the duration of a single
// training event or long-running.
type ContainerLifetime string

func (cl ContainerLifetime) String() string {
	return string(cl)
}

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

	// GetGpusPerHost returns the number of GPUs available on each host.
	GetGpusPerHost() int

	// NumReplicas returns the number of replicas that each kernel should have under the target scheduling Policy.
	NumReplicas() int

	// ResourceBindingMode returns the ResourceBindingMode of the target scheduling Policy.
	//
	// The ResourceBindingMode of a scheduling Policy essentially indicates whether the Policy performs dynamic
	// resource management/allocation or whether resources are instead statically bound to KernelContainer instances
	// for the lifetime of the associated UserSession.
	ResourceBindingMode() ResourceBindingMode

	// GetNewPlacer returns a concrete Placer implementation based on the Policy.
	GetNewPlacer(metricsProvider MetricsProvider) (Placer, error)

	// SupportsMigration returns true if the Policy allows for the migration of one or more replicas of
	// a kernel when no replicas are able to serve a code execution request.
	//
	// If SupportsMigration returns false, then it is up to the client to resubmit the request.
	SupportsMigration() bool

	// ContainerLifetime returns the ContainerLifetime of KernelContainer instances created under the target Policy.
	ContainerLifetime() ContainerLifetime

	// ReuseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
	// placed back into the warm KernelContainer pool, or if it should simply be terminated.
	//
	// ReuseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
	// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
	// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
	//
	// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
	ReuseWarmContainers() bool

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

	// ScalingConfiguration returns the ScalingConfiguration of the target AutoscalingPolicy.
	ScalingConfiguration() *ScalingConfiguration

	// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
	// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
	SupportsPredictiveAutoscaling() bool

	// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
	// Adjust the Cluster's capacity as directed by scaling policy.
	ValidateCapacity(cluster Cluster)

	// SmrEnabled returns a flag indicating whether the kernel containers should participate in SMR.
	// This is generally only enabled for the static and dynamic policies.
	SmrEnabled() bool

	// IdleSessionReclamationPolicy returns the IdleSessionReclamationPolicy of the target Policy.
	IdleSessionReclamationPolicy() IdleSessionReclamationPolicy

	// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
	// resource request of an existing/scheduled kernel after it has already been created, or if the
	// initial resource request/allocation is static and cannot be changed after the kernel is created.
	SupportsDynamicResourceAdjustments() bool
}

// IdleSessionReclamationPolicy defines how the scheduling policy handles idle sessions.
type IdleSessionReclamationPolicy interface {
	// IdleSessionReclamationEnabled returns a flag indicating whether the reclamation of idle sessions is enabled.
	IdleSessionReclamationEnabled() bool

	// ReclaimedSessionsMustReplayAllCells returns a flag indicating whether reclaimed sessions that receive a new
	// "execute_request" message must replay all previous cells before handling the new execution request.
	ReclaimedSessionsMustReplayAllCells() bool

	// IdleSessionReclamationInterval returns a time.Duration encoding the amount of time that a session must remain
	// idle before it is eligible for idle reclamation.
	IdleSessionReclamationInterval() time.Duration
}

// ResourceScalingPolicy defines the configuration of resource scaling (i.e., adding and/or removing Host instances ),
// as well as the configuration parameters that tune the scaling behavior.
type ResourceScalingPolicy interface {
	// ScalingOutEnabled returns a bool indicating whether the Cluster can add additional Host instances if
	// manually/explicitly instructed to do so.
	ScalingOutEnabled() bool

	// ScalingInEnabled returns a bool indicating whether the Cluster can remove Host instances if manually/explicitly
	// instructed to do so.
	ScalingInEnabled() bool

	// DisableScalingOut modifies the scaling policy to disallow scaling-out, even if the policy isn't
	// supposed to support scaling out. This is only intended to be used for unit tests.
	DisableScalingOut()

	// EnableScalingOut modifies the scaling policy to enable scaling-out, even if the policy isn't
	// supposed to support scaling out. This is only intended to be used for unit tests.
	EnableScalingOut()
}

// ScalingConfiguration encapsulates the various parameters related to auto-scaling.
type ScalingConfiguration struct {
	// GpusPerHost is the number of virtual GPUs per host.
	GpusPerHost int

	// ScalingFactor defines how many hosts the cluster will provision based on busy TransactionResources.
	// Specifically, a proposed auto-scale-out is computed as:
	//
	// (<Current GPU Load> * <Scaling Factor>) / <GPUs Per Host>
	//
	// This yields a proposed number of hosts (scaled-out from the current number, in theory, unless the scale factor
	// is very small, in which case it may or may not result in a scale-out.)
	ScalingFactor float64

	// ScalingIntervalSec instructs us how often to call UpdateRatio in seconds.
	// Auto-scaling occurs at the end of UpdateRatio.
	// UpdateRatio updates the subscription ratio, which is used to determine the ratio of subscribed GPUs
	// to how many are actually being used (by actively-training kernel replicas).
	// We use that information to inform if we should scale in or out.
	ScalingIntervalSec float64
	ScalingInterval    time.Duration

	// ScalingLimit defines how many hosts the cluster will provision at maximum based on busy TransactionResources.
	ScalingLimit float64

	// MaximumHostsToReleaseAtOnce defines how many hosts the cluster can de-provision during a single scale-in event. This is equivalent to Jingyuan's "scaling-in limit" parameter.
	MaximumHostsToReleaseAtOnce int32

	// ScalingBufferSize is how many extra hosts we provision so that we can quickly scale if needed.
	ScalingBufferSize int32

	// MinimumCapacity is the minimum number of nodes we must have available at any time.
	MinimumCapacity int32

	// MaximumCapacity is the maximum number of nodes we may have available at any time. If this value is < 0, then it is unbounded.
	MaximumCapacity int32
}

// NewScalingConfiguration creates a new ScalingConfiguration struct, populating its field with the corresponding
// fields from the given SchedulerOptions struct, and returns a pointer to the new ScalingConfiguration struct.
func NewScalingConfiguration(opts *SchedulerOptions) *ScalingConfiguration {
	if opts == nil {
		panic("SchedulerOptions cannot be nil when creating a new ScalingConfiguration struct")
	}

	gpusPerHost := opts.GpusPerHost
	if gpusPerHost <= 0 {
		fmt.Printf(utils.RedStyle.Render("Invalid number of simulated GPUs specified: %d. Value must be >= 1 (even if there are no real GPUs available).\n"),
			gpusPerHost)
		panic(fmt.Sprintf("invalid number of simulated GPUs specified: %d. Value must be >= 1 (even if there are no real GPUs available).",
			gpusPerHost))
	}

	return &ScalingConfiguration{
		GpusPerHost:                 gpusPerHost,
		ScalingFactor:               opts.ScalingFactor,
		MaximumHostsToReleaseAtOnce: int32(opts.MaximumHostsToReleaseAtOnce),
		ScalingIntervalSec:          opts.ScalingIntervalSec,
		ScalingInterval:             time.Millisecond * time.Duration(1000*opts.ScalingIntervalSec),
		ScalingLimit:                opts.ScalingLimit,
		// PredictiveAutoscalingEnabled: opts.PredictiveAutoscalingEnabled,
		ScalingBufferSize: int32(opts.ScalingBufferSize),
		MinimumCapacity:   int32(opts.MinimumNumNodes),
		MaximumCapacity:   int32(opts.MaximumNumNodes),
	}
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
