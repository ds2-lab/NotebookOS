package daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	ConsecutiveFailuresWarning int = 1
	ConsecutiveFailuresBad     int = 2
)

var (
	errRestoreRequired        = errors.New("restore required")
	errExpectingHostScheduler = errors.New("expecting LocalDaemonClient")
	errNodeNameUnspecified    = errors.New("no kubernetes node name returned for LocalDaemonClient")
)

type LocalDaemonClient struct {
	gateway.LocalGatewayClient
	*scheduling.BaseHost

	// The latest GPU info of this host scheduler.
	latestGpuInfo          *gateway.GpuInfo
	gpuInfoMutex           sync.Mutex
	gpuInfoRefreshInterval time.Duration

	log logger.Logger
}

// NewHostScheduler creates a new Host representing a node with a Local Daemon on it.
// Such nodes are capable of hosting replicas.
//
// NewHostScheduler will return an errRestoreRequired error if the IDs don't match.
// NewHostScheduler will return an errNodeNameUnspecified error if there is no NodeName returned by the scheduler.
// If both these errors occur, then only a errNodeNameUnspecified will be returned.
func NewHostScheduler(addr string, conn *grpc.ClientConn, gpuInfoRefreshInterval time.Duration, errorCallback scheduling.ErrorCallback, cpus int32, memMb int32) (*LocalDaemonClient, error) {
	// Generate new ID.
	id := uuid.New().String()

	// Create gRPC client.
	localGatewayClient := gateway.NewLocalGatewayClient(conn)

	// Set the ID. If this fails, the creation of a new host scheduler fails.
	confirmedId, err := localGatewayClient.SetID(context.Background(), &gateway.HostId{Id: id})

	// Validate the response if there's no explicit error.
	if err == nil {
		if confirmedId.NodeName == "" {
			err = errNodeNameUnspecified
		} else if confirmedId.Id != id {
			err = errRestoreRequired
		}
	}

	// If error is now non-nil, either because there was an explicit error or because the response was invalid,
	// then the host scheduler creation failed, and we return nil and the error.
	if err != nil {
		return nil, err
	}

	// Get the initial GPU info. If this fails, the creation of a new host scheduler fails.
	gpuInfoResp, err := localGatewayClient.GetActualGpuInfo(context.Background(), &gateway.Void{})
	if err != nil {
		return nil, err
	}

	// Create the ResourceSpec defining the resources available on the Host.
	resourceSpec := &gateway.ResourceSpec{
		Gpu:    gpuInfoResp.SpecGPUs,
		Cpu:    cpus,
		Memory: memMb,
	}

	// Create the BaseHost.
	baseHost := scheduling.NewBaseHost(confirmedId.Id, confirmedId.NodeName, addr, resourceSpec, nil, conn, errorCallback)

	// Create the LocalDaemonClient.
	scheduler := &LocalDaemonClient{
		BaseHost:               baseHost,
		LocalGatewayClient:     localGatewayClient,
		gpuInfoRefreshInterval: gpuInfoRefreshInterval,
		latestGpuInfo:          gpuInfoResp,
	}

	config.InitLogger(&scheduler.log, scheduler)

	// Start the goroutine that polls for updated GPU info on an interval.
	go scheduler.pollForGpuInfo()

	return scheduler, err
}

// pollForGpuInfo runs a loop that continuously requests GPU usage statistics from all the host schedulers.
func (c *LocalDaemonClient) pollForGpuInfo() {
	numConsecutiveFailures := 0
	for {
		resp, err := c.LocalGatewayClient.GetActualGpuInfo(context.Background(), &gateway.Void{})
		if err != nil {
			c.log.Error("Failed to refresh GPU info from Scheduler %s on Node %s: %v", c.ID(), c.NodeName(), err)
			numConsecutiveFailures += 1

			// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
			if numConsecutiveFailures >= ConsecutiveFailuresWarning {
				// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
				if c.Conn().GetState() == connectivity.TransientFailure || c.Conn().GetState() == connectivity.Shutdown {
					errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.", numConsecutiveFailures, c.ID(), c.NodeName(), c.Conn().GetState().String(), c.ID())
					c.log.Error(errorMessage)
					_ = c.BaseHost.ErrorCallback()(c.ID(), c.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					return
				} else if numConsecutiveFailures >= ConsecutiveFailuresBad { // If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
					errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %c. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.", numConsecutiveFailures, c.ID(), c.NodeName(), c.Conn().GetState().String(), c.ID())
					c.log.Error(errorMessage)
					_ = c.BaseHost.ErrorCallback()(c.ID(), c.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					return
				} else { // Otherwise, we won't assume it is dead yet...
					c.log.Warn("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...", numConsecutiveFailures, c.ID(), c.NodeName(), c.Conn().GetState().String())
				}
			}

			// Sleep for a shorter period of time in order to detect failure more quickly.
			// We'll sleep for longer depending on the number of consecutive failures we've encountered.
			// We clamp the maximum sleep to the standard refresh interval.
			shortenedSleepInterval := time.Duration(math.Max(float64(c.gpuInfoRefreshInterval), float64((time.Second*2)*time.Duration(numConsecutiveFailures))))
			time.Sleep(shortenedSleepInterval)
		} else {
			c.gpuInfoMutex.Lock()
			c.latestGpuInfo = resp

			c.Stats().IdleGPUsStat().Store(float64(resp.IdleGPUs))
			c.Stats().PendingGPUsStat().Store(float64(resp.PendingGPUs))
			c.Stats().CommittedGPUsStat().Store(float64(resp.CommittedGPUs))
			if c.ResourceSpec().GPU() != float64(resp.SpecGPUs) {
				c.log.Warn("Current spec GPUs (%.0f) do not match latest result from polling (%.0f). Updating spec now...", c.ResourceSpec().GPU(), resp.SpecGPUs)
				c.ResourceSpec().UpdateSpecGPUs(float64(resp.SpecGPUs))
			}

			c.gpuInfoMutex.Unlock()

			numConsecutiveFailures = 0
			time.Sleep(c.gpuInfoRefreshInterval)
		}
	}
}

func (c *LocalDaemonClient) String() string {
	return fmt.Sprintf("LocalDaemonClient[Addr: %s, ID: %s]", c.Addr(), c.ID())
}

func (c *LocalDaemonClient) Restore(scheduler scheduling.Host, callback scheduling.ErrorCallback) error {
	restored, ok := scheduler.(*LocalDaemonClient)
	if !ok {
		return errExpectingHostScheduler
	}

	c.BaseHost = restored.BaseHost
	c.BaseHost.SetErrorCallback(callback)
	c.LocalGatewayClient = restored.LocalGatewayClient
	c.latestGpuInfo = restored.latestGpuInfo
	c.gpuInfoRefreshInterval = restored.gpuInfoRefreshInterval

	// TODO: Make sure the other goroutine is no longer active.
	go c.pollForGpuInfo()

	return nil
}

// Stats returns the statistics of the host.
func (c *LocalDaemonClient) Stats() scheduling.HostStatistics {
	return c.BaseHost
}
