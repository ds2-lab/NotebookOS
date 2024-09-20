package scheduling

import (
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

type KubernetesCluster struct {
	*BaseCluster
}

// NewKubernetesCluster creates a new BaseCluster struct and returns a pointer to it.
//
// NewKubernetesCluster should be used when the system is deployed in Kubernetes mode.
// This function accepts parameters that are used to construct a KubernetesScheduler to be used internally
// by the Cluster for scheduling decisions and to respond to scheduling requests by the Kubernetes Scheduler.
func NewKubernetesCluster(gatewayDaemon ClusterGateway, kubeClient KubeClient, hostSpec types.Spec, opts *ClusterSchedulerOptions) *KubernetesCluster {
	baseCluster := newBaseCluster(opts.GpusPerHost)

	kubernetesCluster := &KubernetesCluster{
		BaseCluster: baseCluster,
	}

	placer, err := NewRandomPlacer(baseCluster, opts)
	if err != nil {
		kubernetesCluster.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}
	kubernetesCluster.placer = placer

	scheduler, err := NewKubernetesScheduler(gatewayDaemon, kubernetesCluster, placer, hostSpec, kubeClient, opts)
	if err != nil {
		kubernetesCluster.log.Error("Failed to create Kubernetes Cluster Scheduler: %v", err)
		panic(err)
	}

	kubernetesCluster.scheduler = scheduler
	baseCluster.instance = kubernetesCluster

	return kubernetesCluster
}

func (c *KubernetesCluster) RequestHost(spec types.Spec) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *KubernetesCluster) ReleaseHost(id string) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *KubernetesCluster) HandleScaleInOperation(op *ScaleOperation) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *KubernetesCluster) HandleScaleOutOperation(op *ScaleOperation) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}
