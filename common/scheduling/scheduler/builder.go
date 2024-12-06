package scheduler

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"time"
)

type baseSchedulerBuilder struct {
	cluster            scheduling.Cluster
	placer             scheduling.Placer
	hostMapper         HostMapper
	hostSpec           types.Spec
	kernelProvider     KernelProvider
	notificationBroker NotificationBroker
	schedulingPolicy   scheduling.Policy // Optional, will be extracted from Options if not specified.
	options            *scheduling.SchedulerOptions
}

func newBaseSchedulerBuilder() *baseSchedulerBuilder {
	return &baseSchedulerBuilder{}
}

func (b *baseSchedulerBuilder) WithCluster(cluster scheduling.Cluster) *baseSchedulerBuilder {
	b.cluster = cluster
	return b
}

func (b *baseSchedulerBuilder) WithPlacer(placer scheduling.Placer) *baseSchedulerBuilder {
	b.placer = placer
	return b
}

func (b *baseSchedulerBuilder) WithHostMapper(hostMapper HostMapper) *baseSchedulerBuilder {
	b.hostMapper = hostMapper
	return b
}

func (b *baseSchedulerBuilder) WithHostSpec(hostSpec types.Spec) *baseSchedulerBuilder {
	b.hostSpec = hostSpec
	return b
}

func (b *baseSchedulerBuilder) WithSchedulingPolicy(schedulingPolicy scheduling.Policy) *baseSchedulerBuilder {
	b.schedulingPolicy = schedulingPolicy
	return b
}

func (b *baseSchedulerBuilder) WithKernelProvider(kernelProvider KernelProvider) *baseSchedulerBuilder {
	b.kernelProvider = kernelProvider
	return b
}

func (b *baseSchedulerBuilder) WithNotificationBroker(notificationBroker NotificationBroker) *baseSchedulerBuilder {
	b.notificationBroker = notificationBroker
	return b
}

func (b *baseSchedulerBuilder) WithOptions(options *scheduling.SchedulerOptions) *baseSchedulerBuilder {
	b.options = options
	return b
}

// Build method
func (b *baseSchedulerBuilder) Build() *BaseScheduler {
	if b.options == nil {
		panic("Cannot construct BaseScheduler using baseSchedulerBuilder with nil options.")
	}

	if b.schedulingPolicy == nil {
		schedulingPolicy, err := policy.GetSchedulingPolicy(b.options)
		if err != nil {
			panic(err)
		}

		b.schedulingPolicy = schedulingPolicy
	}

	clusterScheduler := &BaseScheduler{
		cluster:                                  b.cluster,
		hostMapper:                               b.hostMapper,
		stRatio:                                  types.NewMovingStatFromWindow(5),
		opts:                                     b.options,
		remoteSynchronizationInterval:            time.Second * time.Duration(b.options.GpuPollIntervalSeconds),
		placer:                                   b.placer,
		hostSpec:                                 b.hostSpec,
		oversubscribed:                           make(types.Heap, 0, 10),
		undersubscribed:                          make(types.Heap, 0, 10),
		idleHosts:                                make(types.Heap, 0, 10),
		maxSubscribedRatio:                       decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		subscriptionRatio:                        decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		activeAddReplicaOpsPerKernel:             hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]](64),
		addReplicaOperationsByKernelReplicaId:    hashmap.NewCornelkMap[string, *scheduling.AddReplicaOperation](64),
		addReplicaNewPodOrContainerNotifications: hashmap.NewCornelkMap[string, chan *scheduling.AddReplicaOperation](64),
		kernelProvider:                           b.kernelProvider,
		notificationBroker:                       b.notificationBroker,
		schedulingPolicy:                         b.schedulingPolicy,
		//gpusPerHost:                              float64(b.options.GpusPerHost),
		//virtualGpusPerHost:                       int32(b.options.VirtualGpusPerHost),
		//scalingFactor:                            b.options.ScalingFactor,
		//scalingLimit:                             b.options.ScalingLimit,
		//scalingInterval:                          time.Second * time.Duration(b.options.ScalingInterval),
		//maximumHostsToReleaseAtOnce:              int32(b.options.MaximumHostsToReleaseAtOnce),
		//scalingIntervalSec:                       int32(b.options.ScalingInterval),
		//predictiveAutoscalingEnabled:             b.options.PredictiveAutoscalingEnabled,
		//scalingBufferSize:                        int32(b.options.ScalingBufferSize),
		//maximumCapacity:                          int32(b.options.MaximumNumNodes),
		//minimumCapacity:                          int32(b.options.MinimumNumNodes),
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	if b.options.GpuPollIntervalSeconds <= 0 {
		clusterScheduler.remoteSynchronizationInterval = time.Second * 5
	}

	if clusterScheduler.log.GetLevel() == logger.LOG_LEVEL_ALL {
		clusterScheduler.log.Debug("Scheduling Configuration:")
		clusterScheduler.log.Debug("GpusPerHost: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().GpusPerHost)
		clusterScheduler.log.Debug("ScalingFactor: %.2f",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingFactor)
		clusterScheduler.log.Debug("ScalingLimit: %.2f",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingLimit)
		clusterScheduler.log.Debug("MaximumHostsToReleaseAtOnce: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().MaximumHostsToReleaseAtOnce)
		clusterScheduler.log.Debug("ScalingInterval: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec)
		clusterScheduler.log.Debug("PredictiveAutoscalingEnabled: %v",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().PredictiveAutoscalingEnabled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v",
			clusterScheduler.remoteSynchronizationInterval)
		clusterScheduler.log.Debug("Scheduling policy: %v",
			clusterScheduler.schedulingPolicy)
	}

	return clusterScheduler
}
