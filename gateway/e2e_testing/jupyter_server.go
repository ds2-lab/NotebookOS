package e2e_testing

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// JupyterServer is used to simulate a real Jupyter server that forwards requests to kernels through the
// daemon.ClusterGatewayImpl on behalf of clients.
type JupyterServer struct {
	Sockets            []*messaging.Socket
	GatewayProvisioner *GatewayProvisioner

	log logger.Logger
}

func NewJupyterServer(gatewayGrpcAddress string) (*JupyterServer, error) {
	jupyterServer := &JupyterServer{
		Sockets: make([]*messaging.Socket, 0),
	}

	config.InitLogger(&jupyterServer.log, jupyterServer)

	gatewayProvisioner, err := NewGatewayProvisioner(gatewayGrpcAddress)
	if err != nil {
		return nil, err
	}

	jupyterServer.GatewayProvisioner = gatewayProvisioner

	return jupyterServer, nil
}

// GatewayProvisioner is used by the JupyterServer to connect to the daemon.ClusterGatewayImpl and issue gRPC
// requests to create new kernels.
type GatewayProvisioner struct {
	LocalGatewayClient proto.LocalGatewayClient
	Conn               *grpc.ClientConn
	GatewayGrpcAddress string

	log logger.Logger
}

func NewGatewayProvisioner(gatewayGrpcAddress string) (*GatewayProvisioner, error) {
	gatewayProvisioner := &GatewayProvisioner{
		GatewayGrpcAddress: gatewayGrpcAddress,
	}

	config.InitLogger(&gatewayProvisioner.log, gatewayProvisioner)

	var err error
	gatewayProvisioner.Conn, err = grpc.NewClient(gatewayGrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gatewayProvisioner.log.Error("Failed to connect to cluster Gateway via gRPC: %v", err)
		return nil, err
	}

	gatewayProvisioner.LocalGatewayClient = proto.NewLocalGatewayClient(gatewayProvisioner.Conn)

	return gatewayProvisioner, nil
}

func (p *GatewayProvisioner) Close() error {
	return p.Conn.Close()
}
