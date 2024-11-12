package daemon

import (
	"context"
	"errors"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"net"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/hashicorp/yamux"
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
	proto.ClusterGatewayClient
	ready chan struct{}
	log   logger.Logger
}

func NewProvisioner(conn net.Conn) (*Provisioner, *grpc.ClientConn, error) {
	// Initialize yamux session for bidirectional gRPC calls
	// At host scheduler side, a connection replacement first made, then we wait for reverse connection by implementing net.Listener
	srvSession, err := yamux.Server(conn, yamux.DefaultConfig())
	if err != nil {
		return nil, nil, err
	}

	provisioner := &Provisioner{
		Listener: srvSession,
		ready:    make(chan struct{}),
	}
	config.InitLogger(&provisioner.log, provisioner)

	// Initialize the gRPC client
	var grpcClientConn *grpc.ClientConn
	if grpcClientConn, err = provisioner.InitClient(srvSession); err != nil {
		return nil, nil, err
	}

	return provisioner, grpcClientConn, nil
}

// Accept overrides the default Accept method and initializes the gRPC client
func (p *Provisioner) Accept() (conn net.Conn, err error) {
	conn, err = p.Listener.Accept()
	if err != nil {
		p.log.Error("Failed to accept connection: %v", err)
		return nil, err
	}

	p.log.Debug("Accepted connection. RemoteAddr: %v. LocalAddr: %v", conn.RemoteAddr(), conn.LocalAddr())

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
func (p *Provisioner) InitClient(session *yamux.Session) (*grpc.ClientConn, error) {
	// Dial to create a gRPC connection with dummy dialer.
	gConn, err := grpc.Dial(":0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := session.Open()
			if err != nil {
				p.log.Error("Failed to open CLI session during dial: %v", err)
			} else {
				p.log.Debug("Opened cliSession. conn.LocalAddr(): %v, conn.RemoteAddr(): %v", conn.LocalAddr(), conn.RemoteAddr())
			}

			return conn, err
		}))
	if err != nil {
		p.log.Error("Failed to create a gRPC connection using dummy dialer: %v", err)
		return nil, err
	}

	p.log.Debug("Successfully created gRPC connection using dummy dialer. Target: %v", gConn.Target())

	p.ClusterGatewayClient = proto.NewClusterGatewayClient(gConn)
	return gConn, nil
}

// Validate validates the provisioner client.
func (p *Provisioner) Validate() error {
	if p.ClusterGatewayClient == nil {
		return ErrProvisionerNotInitialized
	}
	// Test the connection
	_, err := p.ClusterGatewayClient.ID(context.Background(), proto.VOID)
	return err
}
