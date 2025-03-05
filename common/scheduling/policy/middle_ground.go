package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"golang.org/x/net/context"
	"math"
)

// MiddleGroundPolicy defines the scheduling policy of the "middle ground" approach, in which training requests
// are served exclusively by a pool of warm scheduling.KernelContainer instances.
//
// Of course, if there are no warm scheduling.KernelContainer instances available when an "execute_request" arrives,
// then a new scheduling.KernelContainer instance will be created on demand to serve the request.
//
// MiddleGroundPolicy is similar to MiddleGroundPolicy with the main difference being that the
// ReuseWarmContainers property of MiddleGroundPolicy is true, whereas MiddleGroundPolicy is false for
// MiddleGroundPolicy. As a result, scheduling.KernelContainer instances are returned to the warm pool after
// use when the MiddleGroundPolicy is active.
//
// In real life, this would present security/privacy concerns; however, MiddleGroundPolicy is not meant to be a
// usable, real-world policy. It is simply meant to reflect a hypothetical policy or situation in which there are some
// warm containers available. A real-world instantiation of MiddleGroundPolicy would potentially rely on some
// other research work to handle the actual provisioning and maintenance of a useful warm scheduling.KernelContainer
// pool. Such a pool would have a good balance of scheduling.KernelContainer instances with different runtime
// dependencies and software environments to satisfy the varied requirements of different users.
type MiddleGroundPolicy struct {
	*baseSchedulingPolicy
}

func NewMiddleGroundPolicy(opts *scheduling.SchedulerOptions, clusterProvider scheduling.ClusterProvider) (*MiddleGroundPolicy, error) {
	// This validation step should have already happened, but just in case, we'll ensure that the maximum number
	// of nodes is unbounded.
	if opts.MaximumNumNodes < 0 {
		opts.MaximumNumNodes = math.MaxInt // Effectively unbounded.
	}

	// The "middle-ground" policy uses a fixed-size cluster atop which a warm container pool is maintained.
	if opts.MinimumNumNodes < opts.MaximumNumNodes {
		// The minimum and maximum number of nodes should be equal since the cluster size is fixed.
		opts.MinimumNumNodes = opts.MaximumNumNodes
	}

	// The "middle-ground" policy uses a fixed-size cluster atop which a warm container pool is maintained.
	if opts.InitialClusterSize < opts.MaximumNumNodes {
		// The initial cluster size should be equal to the maximum cluster size as the cluster size should be fixed.
		opts.InitialClusterSize = opts.MaximumNumNodes
	}

	basePolicy, err := newBaseSchedulingPolicy(opts, true, false, clusterProvider)
	if err != nil {
		return nil, err
	}

	policy := &MiddleGroundPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.MiddleGround.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of MiddleGroundPolicy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
}

// ValidateHostForKernel allows the Policy to perform any policy-specific validation logic to ensure that
// the given Host is viable for serving a replica of the specified Kernel.
func (p *MiddleGroundPolicy) ValidateHostForKernel(host scheduling.Host, _ *proto.KernelSpec, _ bool) (isViable bool, unviabilityReason error) {
	if p.clusterProvider == nil {
		panic("MiddleGroundPolicy: Cannot retrieve instance of Cluster. ClusterProvider is nil.")
	}

	cluster := p.clusterProvider()
	if cluster == nil {
		panic("MiddleGroundPolicy: ClusterProvider returned nil.")
	}

	clusterScheduler := cluster.Scheduler()
	if clusterScheduler == nil {
		panic("MiddleGroundPolicy: Scheduler is nil.")
	}

	containerPrewarmer := clusterScheduler.ContainerPrewarmer()
	if containerPrewarmer == nil {
		panic("MiddleGroundPolicy: ContainerPrewarmer is nil.")
	}

	curr, _ := containerPrewarmer.GetNumPrewarmContainersOnHost(host)
	if curr == 0 {
		return false, fmt.Errorf("%w: host \"%s\" does not have any prewarm containers at the moment",
			scheduling.ErrNoPrewarmContainersAvailable, host.GetNodeName())
	}

	return true, nil
}

// getContainerPrewarmer performs all the null-checks to retrieve the scheduling.ContainerPrewarmer from the
// scheduling.Cluster, panicking if at any point, something is nil (including the scheduling.ContainerPrewarmer).
func (p *MiddleGroundPolicy) getContainerPrewarmer() scheduling.ContainerPrewarmer {
	cluster := getClusterFromPolicy(p)

	scheduler := cluster.Scheduler()
	if scheduler == nil {
		panic("Scheduler is nil.")
	}

	containerPrewarmer := scheduler.ContainerPrewarmer()
	if containerPrewarmer == nil {
		panic("ContainerPrewarmer is nil.")
	}

	return containerPrewarmer
}

// HandleFailedAttemptToGetViableHosts is called when the Scheduler fails to find the requested number of Host
// instances to serve the KernelReplica instance(s) of a particular Kernel.
func (p *MiddleGroundPolicy) HandleFailedAttemptToGetViableHosts(_ context.Context, kernelSpec *proto.KernelSpec,
	numHosts int32, hosts []scheduling.Host) (bool, error) {

	containerPrewarmer := p.getContainerPrewarmer()

	hostMap := make(map[string]scheduling.Host, len(hosts))
	for _, host := range hosts {
		hostMap[host.GetID()] = host
	}

	kernelResourceSpec := kernelSpec.ResourceSpec.ToDecimalSpec()

	// Note that a host that passes these criteria could fail to do so later.
	// This isn't a promise/guarantee. Likewise, a host that fails could later be eligible.
	criteriaFunc := func(host scheduling.Host) error {
		if _, ok := hostMap[host.GetID()]; ok {
			return fmt.Errorf("host \"%s\" is already selected as a candidate host for kernel \"%s\"",
				host.GetNodeName(), kernelSpec.Id)
		}

		if !host.CanServeContainer(kernelResourceSpec) {
			return fmt.Errorf("host \"%s\" does not have sufficient resources to serve kernel \"%s\"",
				host.GetNodeName(), kernelSpec.Id)
		}

		if !host.CanCommitResources(kernelResourceSpec) {
			return fmt.Errorf("host \"%s\" does not have sufficient idle resources to commit to kernel \"%s\"",
				host.GetNodeName(), kernelSpec.Id)
		}

		return nil // Host is viable (as of right now, at least...)
	}

	provisioned, err := containerPrewarmer.RequestProvisionContainers(int(numHosts), criteriaFunc, true)
	if err != nil {
		p.log.Warn("Error while provisioning prewarm containers on hosts: %v", err)
		return len(provisioned) > 0, err
	}

	return len(provisioned) > 0, nil
}

// ValidateHostForReplica allows the Policy to perform any policy-specific validation logic to ensure that
// the given Host is viable for serving a replica of the specified Kernel.
func (p *MiddleGroundPolicy) ValidateHostForReplica(host scheduling.Host, _ *proto.KernelReplicaSpec, _ bool) (isViable bool, unviabilityReason error) {
	if p.clusterProvider == nil {
		panic("MiddleGroundPolicy: Cannot retrieve instance of Cluster. ClusterProvider is nil.")
	}

	cluster := p.clusterProvider()
	if cluster == nil {
		panic("MiddleGroundPolicy: ClusterProvider returned nil.")
	}

	clusterScheduler := cluster.Scheduler()
	if clusterScheduler == nil {
		panic("MiddleGroundPolicy: Scheduler is nil.")
	}

	containerPrewarmer := clusterScheduler.ContainerPrewarmer()
	if containerPrewarmer == nil {
		panic("MiddleGroundPolicy: ContainerPrewarmer is nil.")
	}

	curr, _ := containerPrewarmer.GetNumPrewarmContainersOnHost(host)
	if curr == 0 {
		return false, fmt.Errorf("%w: host \"%s\" does not have any prewarm containers at the moment",
			scheduling.ErrNoPrewarmContainersAvailable, host.GetNodeName())
	}

	return true, nil
}

// ReuseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
// placed back into the warm KernelContainer pool, or if it should simply be terminated.
//
// ReuseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
//
// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
func (p *MiddleGroundPolicy) ReuseWarmContainers() bool {
	return true
}

// RequirePrewarmContainer indicates whether a new kernel replica must be placed within a prewarm container.
func (p *MiddleGroundPolicy) RequirePrewarmContainer() bool {
	return true
}

// PrioritizePrewarmContainers indicates whether the host selection process should prioritize hosts with
// a prewarm container available or not factor that into the placement decision.
func (p *MiddleGroundPolicy) PrioritizePrewarmContainers() bool {
	return true
}

// SelectReplicaForMigration selects a KernelReplica of the specified kernel to be migrated.
func (p *MiddleGroundPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if p.SupportsMigration() {
		panic("MiddleGroundPolicy isn't supposed to support migration, yet apparently it does?")
	}

	return nil, ErrMigrationNotSupported
}

// FindReadyReplica (optionally) selects a KernelReplica of the specified kernel to be
// pre-designated as the leader of a code execution.
//
// If the returned KernelReplica is nil and the returned error is nil, then that indicates
// that no KernelReplica is being pre-designated as the leader, and the KernelReplicas
// will fight amongst themselves to determine the leader.
//
// If a non-nil KernelReplica is returned, then the "execute_request" messages that are
// forwarded to that KernelReplica's peers should first be converted to "yield_request"
// messages, thereby ensuring that the selected KernelReplica becomes the leader.
//
// FindReadyReplica also returns a map of ineligible replicas, or replicas that have already
// been ruled out.
func (p *MiddleGroundPolicy) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	return defaultFindReadyReplicaSingleReplicaPolicy(p, kernel, executionId)
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *MiddleGroundPolicy) ValidateCapacity(cluster scheduling.Cluster) {
	// Ensure we don't double-up on capacity validations. Only one at a time.
	if !p.isValidatingCapacity.CompareAndSwap(0, 1) {
		return
	}

	singleReplicaValidateCapacity(p, cluster, p.log)

	if !p.isValidatingCapacity.CompareAndSwap(1, 0) {
		panic("Failed to swap isValidatingCapacity 1 → 0 after finishing call to MiddleGroundPolicy::ValidateCapacity")
	}
}

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *MiddleGroundPolicy) SupportsPredictiveAutoscaling() bool {
	return true
}

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *MiddleGroundPolicy) SupportsDynamicResourceAdjustments() bool {
	// TODO: Should this return true or false?
	return true
}

func (p *MiddleGroundPolicy) SmrEnabled() bool {
	return false
}

func (p *MiddleGroundPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.MiddleGround
}

func (p *MiddleGroundPolicy) Name() string {
	return "First-Come, First-Serve Batch Scheduling"
}

func (p *MiddleGroundPolicy) NumReplicas() int {
	return 1
}

func (p *MiddleGroundPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesWhenContainerScheduled
}

func (p *MiddleGroundPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.SingleTrainingEvent
}

func (p *MiddleGroundPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	// MiddleGroundPolicy implements scheduling.PostExecutionStatePolicy directly, so we
	// just return the target MiddleGroundPolicy struct.
	return p
}

func (p *MiddleGroundPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	// MiddleGroundPolicy implements scheduling.PreExecutionStatePolicy directly, so we
	// just return the target MiddleGroundPolicy struct.
	return p
}

func (p *MiddleGroundPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	// MiddleGroundPolicy implements scheduling.ResourceScalingPolicy directly, so we
	// just return the target MiddleGroundPolicy struct.
	return p
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *MiddleGroundPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

func (p *MiddleGroundPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

// ScalingOutEnabled is disabled for the MiddleGroundPolicy, as the MiddleGroundPolicy uses a fixed-size cluster
// on which a warm container pool is created and maintained.
func (p *MiddleGroundPolicy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

// ScalingInEnabled is disabled for the MiddleGroundPolicy, as the MiddleGroundPolicy uses a fixed-size cluster
// on which a warm container pool is created and maintained.
func (p *MiddleGroundPolicy) ScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *MiddleGroundPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *MiddleGroundPolicy) WriteOperationIsOnCriticalPath() bool {
	return true
}

/////////////////////////////////////////////
// PreExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformReadOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network read operation before executing user-submitted code.
//
// Such a read operation would be to retrieve the current or latest model state/parameters and any required
// training data.
func (p *MiddleGroundPolicy) ShouldPerformReadOperation() bool {
	return true
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *MiddleGroundPolicy) ReadOperationIsOnCriticalPath() bool {
	return true
}
