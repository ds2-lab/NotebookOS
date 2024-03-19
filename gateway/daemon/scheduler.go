package daemon

import (
	"context"
	"encoding/json"
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
)

var (
	errRestoreRequired        = errors.New("restore required")
	errExpectingHostScheduler = errors.New("expecting HostScheduler")
)

type HostScheduler struct {
	gateway.LocalGatewayClient
	meta hashmap.BaseHashMap[string, interface{}]

	// The latest GPU info of this host scheduler.
	gpuInfo                *gateway.GpuInfo
	gpuInfoRefreshInterval time.Duration

	log logger.Logger

	id   string
	addr string
	conn *grpc.ClientConn

	gpuInfoMutex sync.Mutex
}

func NewHostScheduler(addr string, conn *grpc.ClientConn, gpuInfoRefreshInterval time.Duration) (*HostScheduler, error) {
	id := uuid.New().String()
	scheduler := &HostScheduler{
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

	if confirmedId.Id != id {
		err = errRestoreRequired
	}
	scheduler.id = confirmedId.Id

	go scheduler.pollForGpuInfo()

	return scheduler, err
}

func (s *HostScheduler) pollForGpuInfo() {
	for {
		resp, err := s.LocalGatewayClient.GetGpuInfo(context.Background(), &gateway.Void{})
		if err != nil {
			s.log.Error("Failed to refresh GPU info: %v", err)
		} else {
			s.gpuInfoMutex.Lock()
			s.gpuInfo = resp
			s.gpuInfoMutex.Unlock()

			gpuInfoJson, _ := json.Marshal(s.gpuInfo)
			s.log.Debug("Refreshed GPU info: %s. Will refresh in %v.", string(gpuInfoJson), s.gpuInfoRefreshInterval)
		}

		time.Sleep(s.gpuInfoRefreshInterval)
	}
}

func (s *HostScheduler) ID() string {
	return s.id
}

func (s *HostScheduler) Addr() string {
	return s.addr
}

func (s *HostScheduler) String() string {
	return fmt.Sprintf("HostScheduler[Addr: %s, ID: %s]", s.addr, s.id)
}

func (s *HostScheduler) Restore(scheduler core.Host) error {
	restored, ok := scheduler.(*HostScheduler)
	if !ok {
		return errExpectingHostScheduler
	}
	s.LocalGatewayClient, s.conn = restored.LocalGatewayClient, restored.conn
	s.addr = restored.addr
	return nil
}

// Stats returns the statistics of the host.
func (s *HostScheduler) Stats() core.HostStats {
	return nil
}

// SetMeta sets the meta data of the host.
func (s *HostScheduler) SetMeta(key core.HostMetaKey, value interface{}) {
	s.meta.Store(string(key), value)
}

// GetMeta return the meta data of the host.
func (s *HostScheduler) GetMeta(key core.HostMetaKey) interface{} {
	if value, ok := s.meta.Load(string(key)); ok {
		return value
	}
	return nil
}
