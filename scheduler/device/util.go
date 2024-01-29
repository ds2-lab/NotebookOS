package device

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// waitForDevicePluginServer checks if the DevicePlugin gRPC server is alive.
// This is done by creating a blocking gRPC connection to the server's socket.
// by making grpc blocking connection to the server socket.
func waitForDevicePluginServer(sock string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	conn, err := grpc.DialContext(ctx, sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)
	if conn != nil {
		_ = conn.Close()
	}

	return errors.Wrapf(err, "Failed gRPC::DialContext for socket \"%s\"", sock)
}
