package scheduler

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/scusemua/distributed-notebook/common/scheduling"
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

	clusterScheduler := &BaseScheduler{
		cluster:                                  b.cluster,
		hostMapper:                               b.hostMapper,
		gpusPerHost:                              float64(b.options.GpusPerHost),
		virtualGpusPerHost:                       int32(b.options.VirtualGpusPerHost),
		scalingFactor:                            b.options.ScalingFactor,
		scalingLimit:                             b.options.ScalingLimit,
		maximumHostsToReleaseAtOnce:              int32(b.options.MaximumHostsToReleaseAtOnce),
		scalingIntervalSec:                       int32(b.options.ScalingInterval),
		predictiveAutoscalingEnabled:             b.options.PredictiveAutoscalingEnabled,
		scalingBufferSize:                        int32(b.options.ScalingBufferSize),
		stRatio:                                  types.NewMovingStatFromWindow(5),
		opts:                                     b.options,
		remoteSynchronizationInterval:            time.Second * time.Duration(b.options.GpuPollIntervalSeconds),
		placer:                                   b.placer,
		hostSpec:                                 b.hostSpec,
		oversubscribed:                           make(types.Heap, 0, 10),
		undersubscribed:                          make(types.Heap, 0, 10),
		idleHosts:                                make(types.Heap, 0, 10),
		maximumCapacity:                          int32(b.options.MaximumNumNodes),
		minimumCapacity:                          int32(b.options.MinimumNumNodes),
		maxSubscribedRatio:                       decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		subscriptionRatio:                        decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		activeAddReplicaOpsPerKernel:             hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]](64),
		addReplicaOperationsByKernelReplicaId:    hashmap.NewCornelkMap[string, *scheduling.AddReplicaOperation](64),
		addReplicaNewPodOrContainerNotifications: hashmap.NewCornelkMap[string, chan *scheduling.AddReplicaOperation](64),
		kernelProvider:                           b.kernelProvider,
		notificationBroker:                       b.notificationBroker,
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	if b.options.GpuPollIntervalSeconds <= 0 {
		clusterScheduler.remoteSynchronizationInterval = time.Second * 5
	}

	if clusterScheduler.scalingIntervalSec < 0 {
		clusterScheduler.scalingIntervalSec = 30
		clusterScheduler.scalingInterval = time.Second * time.Duration(30)
	}

	switch b.options.SchedulingPolicy {
	case string(scheduling.DefaultSchedulingPolicy):
		{
			clusterScheduler.schedulingPolicy = scheduling.DefaultSchedulingPolicy
		}
	case string(scheduling.Static):
		{
			clusterScheduler.schedulingPolicy = scheduling.Static
		}
	case string(scheduling.DynamicV3):
		{
			clusterScheduler.schedulingPolicy = scheduling.DynamicV3
		}
	case string(scheduling.DynamicV4):
		{
			clusterScheduler.schedulingPolicy = scheduling.DynamicV4
		}
	case string(scheduling.FcfsBatch):
		{
			clusterScheduler.schedulingPolicy = scheduling.FcfsBatch
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", b.options.SchedulingPolicy))
		}
	}

	if clusterScheduler.log.GetLevel() == logger.LOG_LEVEL_ALL {
		clusterScheduler.log.Debug("Scheduling Configuration:")
		clusterScheduler.log.Debug("GpusPerHost: %.2f", clusterScheduler.gpusPerHost)
		clusterScheduler.log.Debug("VirtualGpusPerHost: %d", clusterScheduler.virtualGpusPerHost)
		clusterScheduler.log.Debug("ScalingFactor: %.2f", clusterScheduler.scalingFactor)
		clusterScheduler.log.Debug("ScalingLimit: %.2f", clusterScheduler.scalingLimit)
		clusterScheduler.log.Debug("MaximumHostsToReleaseAtOnce: %d", clusterScheduler.maximumHostsToReleaseAtOnce)
		clusterScheduler.log.Debug("ScalingInterval: %d", clusterScheduler.scalingIntervalSec)
		clusterScheduler.log.Debug("PredictiveAutoscalingEnabled: %v", clusterScheduler.predictiveAutoscalingEnabled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d", clusterScheduler.scalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v", clusterScheduler.remoteSynchronizationInterval)
		clusterScheduler.log.Debug("Scheduling policy: %v", clusterScheduler.schedulingPolicy)
	}

	return clusterScheduler
}
