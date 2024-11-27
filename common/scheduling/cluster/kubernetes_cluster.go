package cluster

import (
	"fmt"
	"github.com/Scusemua/go-utils/promise"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
)

type KubernetesCluster struct {
	*BaseCluster
}

func (c *KubernetesCluster) MetricsProvider() scheduling.MetricsProvider {
	//TODO implement me
	panic("implement me")
}

func (c *KubernetesCluster) Sessions() hashmap.HashMap[string, scheduling.UserSession] {
	//TODO implement me
	panic("implement me")
}

func (c *KubernetesCluster) GetIndex(category string, expected interface{}) (scheduling.IndexProvider, bool) {
	//TODO implement me
	panic("implement me")
}

func (c *KubernetesCluster) Scheduler() scheduling.Scheduler {
	//TODO implement me
	panic("implement me")
}

// NewKubernetesCluster creates a new BaseCluster struct and returns a pointer to it.
//
// NewKubernetesCluster should be used when the system is deployed in Kubernetes mode.
// This function accepts parameters that are used to construct a KubernetesScheduler to be used internally
// by the Cluster for scheduling decisions and to respond to scheduling requests by the Kubernetes Scheduler.
func NewKubernetesCluster(kubeClient scheduling.KubeClient, hostSpec types.Spec, placer scheduling.Placer, hostMapper scheduler.HostMapper,
	kernelProvider scheduler.KernelProvider, clusterMetricsProvider scheduling.MetricsProvider,
	notificationBroker scheduler.NotificationBroker, schedulingPolicy scheduling.Policy, opts *scheduling.SchedulerOptions) *KubernetesCluster {

	baseCluster := newBaseCluster(opts, placer, clusterMetricsProvider, "KubernetesCluster")
	kubernetesCluster := &KubernetesCluster{
		BaseCluster: baseCluster,
	}

	kubeScheduler, err := scheduler.NewKubernetesScheduler(kubernetesCluster, placer, hostMapper, kernelProvider,
		hostSpec, kubeClient, notificationBroker, schedulingPolicy, opts)

	if err != nil {
		kubernetesCluster.log.Error("Failed to create Kubernetes Cluster Scheduler: %v", err)
		panic(err)
	}

	kubernetesCluster.scheduler = kubeScheduler
	baseCluster.instance = kubernetesCluster

	return kubernetesCluster
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *KubernetesCluster) NodeType() string {
	return types.KubernetesNode
}

func (c *KubernetesCluster) RequestHost(_ types.Spec) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

// CanPossiblyScaleOut returns true if the Cluster could possibly scale-out.
// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
// it is currently not supported unless there is at least one disabled host already within the cluster.
func (c *KubernetesCluster) CanPossiblyScaleOut() bool {
	// For now, this is never supported for Kubernetes clusters.
	return false
}

func (c *KubernetesCluster) ReleaseHost(_ string) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *KubernetesCluster) HandleScaleInOperation(_ *scheduler.ScaleOperation) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *KubernetesCluster) HandleScaleOutOperation(_ *scheduler.ScaleOperation) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
func (c *KubernetesCluster) GetScaleOutCommand(_ int32, _ chan interface{}) func() {
	panic(fmt.Errorf("%w: KubernetesCluster::GetScaleOutCommand", scheduling.ErrNotImplementedYet))
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
func (c *KubernetesCluster) GetScaleInCommand(_ int32, _ []string, _ chan interface{}) (func(), error) {
	panic(fmt.Errorf("%w: KubernetesCluster::GetScaleInCommand", scheduling.ErrNotImplementedYet))
}
