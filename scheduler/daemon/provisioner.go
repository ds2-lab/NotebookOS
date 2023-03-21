package daemon

import (
	"context"
	"errors"
	"net"

	"github.com/hashicorp/yamux"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrProvisionerNotInitialized = errors.New("provisioner is not initialized")
)

// Provisioner is a wrapper of gateway.ClusterGatewayClient that accepts reverse connection and
// initializes the gRPC client. The Accept method should be called only once.
type Provisioner struct {
	net.Listener
	gateway.ClusterGatewayClient
	ready chan struct{}
	log   logger.Logger
}

func NewProvisioner(conn net.Conn) (*Provisioner, error) {
	// Initialize yamux session for bi-directional gRPC calls
	// At host scheduler side, a connection replacement first made, then we wait for reverse connection by implementing net.Listener
	srvSession, err := yamux.Server(conn, yamux.DefaultConfig())
	if err != nil {
		return nil, err
	}

	provisioner := &Provisioner{
		Listener: srvSession,
		ready:    make(chan struct{}),
	}
	config.InitLogger(&provisioner.log, provisioner)

	// Initialize the gRPC client
	if err := provisioner.InitClient(srvSession); err != nil {
		return nil, err
	}

	return provisioner, nil
}

// Accept overrides the default Accept method and initializes the gRPC client
func (p *Provisioner) Accept() (conn net.Conn, err error) {
	conn, err = p.Listener.Accept()
	if err != nil {
		return nil, err
	}

	// Notify possible blocking caller that the gRPC client is initialized
	go func() {
		select {
		case p.ready <- struct{}{}:
			p.log.Info("Provisioner is ready.")
		default:
			p.log.Warn("Unexpected duplicated reverse provisioner connection.")
		}
	}()
	return conn, nil
}

// Ready returns a channel that is closed when the gRPC client is initialized
func (p *Provisioner) Ready() <-chan struct{} {
	return p.ready
}

// InitClient initializes the gRPC client
func (p *Provisioner) InitClient(session *yamux.Session) error {
	// Dial to create a gRPC connection with dummy dialer.
	gConn, err := grpc.Dial(":0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return session.Open()
		}))
	if err != nil {
		return err
	}

	p.ClusterGatewayClient = gateway.NewClusterGatewayClient(gConn)
	return nil
}

// Validate validates the provisioner client.
func (p *Provisioner) Validate() error {
	if p.ClusterGatewayClient == nil {
		return ErrProvisionerNotInitialized
	}
	// Test the connection
	_, err := p.ClusterGatewayClient.ID(context.Background(), gateway.VOID)
	return err
}
