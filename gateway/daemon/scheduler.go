package daemon

import (
	"context"
	"errors"

	"github.com/google/uuid"
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

	id   string
	addr string
	conn *grpc.ClientConn
}

func NewHostScheduler(addr string, conn *grpc.ClientConn) (*HostScheduler, error) {
	id := uuid.New().String()
	scheduler := &HostScheduler{
		LocalGatewayClient: gateway.NewLocalGatewayClient(conn),
		addr:               addr,
		conn:               conn,
		meta:               hashmap.NewCornelkMap[string, interface{}](10),
	}

	confirmedId, err := scheduler.SetID(context.Background(), &gateway.HostId{Id: id})
	if err != nil {
		return nil, err
	}

	if confirmedId.Id != id {
		err = errRestoreRequired
	}
	scheduler.id = confirmedId.Id
	return scheduler, err
}

func (s *HostScheduler) ID() string {
	return s.id
}

func (s *HostScheduler) Addr() string {
	return s.addr
}

func (s *HostScheduler) String() string {
	return s.addr
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
