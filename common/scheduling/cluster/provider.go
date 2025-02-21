package cluster

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
)

const (
	DockerSwarm   Type = "DockerSwarmCluster"
	DockerCompose Type = "DockerCluster"
	Kubernetes    Type = "KubernetesCluster"
)

var (
	ErrMissingRequiredArgument = errors.New("required argument was not specified before trying to build the cluster")
)

type Type string

func (t Type) String() string {
	return string(t)
}

// Provider provides an interface for creating a scheduling.Cluster struct of the proper type based
// on the configured ClusterType.
//
// Provider validates that all required arguments are non-nil before creating the scheduling.Cluster.
type Provider struct {
	HostSpec               types.Spec                   // Required.
	Placer                 scheduling.Placer            // Required.
	HostMapper             scheduler.HostMapper         // Required.
	KernelProvider         scheduler.KernelProvider     // Required.
	ClusterMetricsProvider scheduling.MetricsProvider   // Optional.
	NotificationBroker     scheduler.NotificationBroker // Optional.
	SchedulingPolicy       scheduling.Policy            // Optional, will be extracted from Options if not specified.
	KubeClient             scheduling.KubeClient        // Required for Kubernetes clusters. Ignored for others.

	log                       logger.Logger
	Options                   *scheduling.SchedulerOptions                      // Required.
	StatisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics)) // Optional.

	ClusterType Type // Required.
}

func NewBuilder(clusterType Type) *Provider {
	provider := &Provider{
		ClusterType: clusterType,
	}

	config.InitLogger(&provider.log, provider)

	return provider
}

func (b *Provider) WithClusterType(t Type) *Provider {
	b.ClusterType = t
	return b
}

func (b *Provider) WithHostSpec(t types.Spec) *Provider {
	b.HostSpec = t
	return b
}

func (b *Provider) WithPlacer(sp scheduling.Placer) *Provider {
	b.Placer = sp
	return b
}

func (b *Provider) WithSchedulingPolicy(sp scheduling.Policy) *Provider {
	b.SchedulingPolicy = sp
	return b
}

func (b *Provider) WithHostMapper(hm scheduler.HostMapper) *Provider {
	b.HostMapper = hm
	return b
}

func (b *Provider) WithKernelProvider(kp scheduler.KernelProvider) *Provider {
	b.KernelProvider = kp
	return b
}

func (b *Provider) WithClusterMetricsProvider(mp scheduling.MetricsProvider) *Provider {
	b.ClusterMetricsProvider = mp
	return b
}

func (b *Provider) WithNotificationBroker(nb scheduler.NotificationBroker) *Provider {
	b.NotificationBroker = nb
	return b
}

func (b *Provider) WithStatisticsUpdateProvider(sup func(func(statistics *metrics.ClusterStatistics))) *Provider {
	b.StatisticsUpdaterProvider = sup
	return b
}

func (b *Provider) WithOptions(o *scheduling.SchedulerOptions) *Provider {
	b.Options = o
	return b
}

func (b *Provider) WithKubeClient(k scheduling.KubeClient) *Provider {
	b.KubeClient = k
	return b
}

// Validate checks that all required arguments have values. If this is the case, then Validate returns nil.
//
// If one or more required arguments are missing a value, then Validate will return an error.
func (b *Provider) Validate() error {
	if b.ClusterType == Kubernetes && b.KubeClient == nil {
		b.log.Error("Cannot create %s cluster. Missing KubeClient argument.", b.ClusterType.String())
		return fmt.Errorf("%w: KubeClient", ErrMissingRequiredArgument)
	}

	if b.HostSpec == nil {
		b.log.Error("Cannot create %s cluster. Missing HostSpec argument.", b.ClusterType.String())
		return fmt.Errorf("%w: HostSpec", ErrMissingRequiredArgument)
	}

	if b.Placer == nil {
		b.log.Error("Cannot create %s cluster. Missing Placer argument.", b.ClusterType.String())
		return fmt.Errorf("%w: Placer", ErrMissingRequiredArgument)
	}

	if b.HostMapper == nil {
		b.log.Error("Cannot create %s cluster. Missing HostMapper argument.", b.ClusterType.String())
		return fmt.Errorf("%w: HostMapper", ErrMissingRequiredArgument)
	}

	if b.KernelProvider == nil {
		b.log.Error("Cannot create %s cluster. Missing KernelProvider argument.", b.ClusterType.String())
		return fmt.Errorf("%w: KernelProvider", ErrMissingRequiredArgument)
	}

	if b.Options == nil {
		b.log.Error("Cannot create %s cluster. Missing ClusterGatewayOptions argument.", b.ClusterType.String())
		return fmt.Errorf("%w: ClusterGatewayOptions", ErrMissingRequiredArgument)
	}

	// We wait to validate the scheduling policy until BuildCluster is called,
	// so that we can create a clusterProvider function.

	b.log.Debug("Successfully validated arguments for %s cluster.", b.ClusterType.String())
	return nil
}

// BuildCluster first validates the arguments before creating a scheduling.Cluster struct of the specified type.
// If the arguments fail to validate (i.e., if a required argument was not specified), then BuildCluster will
// return nil for the scheduling.Cluster and an ErrMissingRequiredArgument error indicating the missing argument.
func (b *Provider) BuildCluster() (scheduling.Cluster, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}

	var (
		clusterPointer *scheduling.Cluster // Interface pointer.
		tmpCluster     scheduling.Cluster
	)

	clusterProvider := func() scheduling.Cluster {
		if clusterPointer == nil {
			return nil
		}

		return *clusterPointer
	}

	if b.SchedulingPolicy == nil {
		schedulingPolicy, err := scheduler.GetSchedulingPolicy(b.Options, clusterProvider)
		if err != nil {
			return nil, err
		}

		b.SchedulingPolicy = schedulingPolicy
	}

	if b.ClusterType == DockerCompose || b.ClusterType == DockerSwarm {
		b.log.Debug("Creating %s cluster now.", b.ClusterType.String())

		tmpCluster = NewDockerCluster(b.HostSpec, b.Placer, b.HostMapper, b.KernelProvider,
			b.ClusterMetricsProvider, b.NotificationBroker, b.SchedulingPolicy, b.StatisticsUpdaterProvider, b.Options)
	}

	if b.ClusterType == Kubernetes {
		b.log.Debug("Creating %s cluster now.", b.ClusterType.String())
		tmpCluster = NewKubernetesCluster(b.KubeClient, b.HostSpec, b.Placer, b.HostMapper, b.KernelProvider,
			b.ClusterMetricsProvider, b.NotificationBroker, b.SchedulingPolicy, b.StatisticsUpdaterProvider, b.Options)
	}

	if tmpCluster == nil {
		b.log.Error("Unknown/unsupported cluster type: %v", b.ClusterType.String())
		log.Fatalf("Unknown/unsupported cluster type: %v\n", b.ClusterType.String())
	}

	clusterPointer = &tmpCluster
	return tmpCluster, nil
}
