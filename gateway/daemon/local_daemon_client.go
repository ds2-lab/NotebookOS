package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	errRestoreRequired        = errors.New("restore required")
	errExpectingHostScheduler = errors.New("expecting LocalDaemonClient")
	errNodeNameUnspecified    = errors.New("no kubernetes node name returned for LocalDaemonClient")
)

type LocalDaemonClient struct {
	gateway.LocalGatewayClient
	meta hashmap.BaseHashMap[string, interface{}]

	// The latest GPU info of this host scheduler.
	gpuInfo                *gateway.GpuInfo
	gpuInfoRefreshInterval time.Duration

	log logger.Logger

	id       string
	addr     string
	nodeName string
	conn     *grpc.ClientConn

	gpuInfoMutex sync.Mutex
}

// This will return an errRestoreRequired error if the IDs don't match.
// This will return an errNodeNameUnspecified error if there is no NodeName returned by the scheduler.
// If both these errors occur, then only a errNodeNameUnspecified will be returned.
func NewHostScheduler(addr string, conn *grpc.ClientConn, gpuInfoRefreshInterval time.Duration) (*LocalDaemonClient, error) {
	id := uuid.New().String()
	scheduler := &LocalDaemonClient{
		LocalGatewayClient:     gateway.NewLocalGatewayClient(conn),
		addr:                   addr,
		conn:                   conn,
		meta:                   hashmap.NewCornelkMap[string, interface{}](10),
		gpuInfoRefreshInterval: gpuInfoRefreshInterval,
	}

	config.InitLogger(&scheduler.log, scheduler)

	confirmedId, err := scheduler.SetID(context.Background(), &gateway.HostId{Id: id})
	if err != nil {
		return nil, err
	}

	if confirmedId.NodeName == "" {
		err = errNodeNameUnspecified
	} else if confirmedId.Id != id {
		err = errRestoreRequired
	}

	scheduler.id = confirmedId.Id
	scheduler.nodeName = confirmedId.NodeName

	go scheduler.pollForGpuInfo()

	return scheduler, err
}

func (s *LocalDaemonClient) pollForGpuInfo() {
	numConsecutiveFailures := 0
	for {
		resp, err := s.LocalGatewayClient.GetActualGpuInfo(context.Background(), &gateway.Void{})
		if err != nil {
			s.log.Error("Failed to refresh GPU info from Scheduler %s on Node %s: %v", s.id, s.nodeName, err)
			numConsecutiveFailures += 1

			// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
			if numConsecutiveFailures >= 3 {
				// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
				if (s.conn.GetState() == connectivity.TransientFailure || s.conn.GetState() == connectivity.Shutdown) {
					s.log.Error("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.", numConsecutiveFailures, s.id, s.nodeName, s.conn.GetState().String(), s.id)
					return 
				} else if numConsecutiveFailures >= 5 { // If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
					s.log.Error("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.", numConsecutiveFailures, s.id, s.nodeName, s.conn.GetState().String(), s.id)
					return
				} else { // Otherwise, we won't asume it is dead yet... 
					s.log.Warn("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...", numConsecutiveFailures, s.id, s.nodeName, s.conn.GetState().String())
				}
			}
		} else {
			s.gpuInfoMutex.Lock()
			s.gpuInfo = resp
			s.gpuInfoMutex.Unlock()

			numConsecutiveFailures = 0
		}

		time.Sleep(s.gpuInfoRefreshInterval)
	}
}

func (s *LocalDaemonClient) ID() string {
	return s.id
}

func (s *LocalDaemonClient) NodeName() string {
	return s.nodeName
}

func (s *LocalDaemonClient) Addr() string {
	return s.addr
}

func (s *LocalDaemonClient) String() string {
	return fmt.Sprintf("LocalDaemonClient[Addr: %s, ID: %s]", s.addr, s.id)
}

func (s *LocalDaemonClient) Restore(scheduler core.Host) error {
	restored, ok := scheduler.(*LocalDaemonClient)
	if !ok {
		return errExpectingHostScheduler
	}
	s.LocalGatewayClient, s.conn = restored.LocalGatewayClient, restored.conn
	s.addr = restored.addr
	return nil
}

// Stats returns the statistics of the host.
func (s *LocalDaemonClient) Stats() core.HostStats {
	return nil
}

// SetMeta sets the meta data of the host.
func (s *LocalDaemonClient) SetMeta(key core.HostMetaKey, value interface{}) {
	s.meta.Store(string(key), value)
}

// GetMeta return the meta data of the host.
func (s *LocalDaemonClient) GetMeta(key core.HostMetaKey) interface{} {
	if value, ok := s.meta.Load(string(key)); ok {
		return value
	}
	return nil
}
