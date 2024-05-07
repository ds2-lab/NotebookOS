package consul

import (
	"fmt"
	"net"
	"os"

	consul "github.com/hashicorp/consul/api"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

// NewClient returns a new Client with connection to consul
func NewClient(addr string) (*Client, error) {
	cfg := consul.DefaultConfig()
	cfg.Address = addr

	c, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	cli := &Client{Client: c}
	config.InitLogger(&cli.logger, "Consul ")

	return cli, nil
}

// Client provides an interface for communicating with registry
type Client struct {
	*consul.Client

	logger logger.Logger
}

// Look for the network device being dedicated for gRPC traffic.
// The network CDIR should be specified in os environment
// "DSB_HOTELRESERV_GRPC_NETWORK".
// If not found, return the first non loopback IP address.
func (c *Client) getLocalIP() (string, error) {
	var ipGrpc string
	var ips []net.IP

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP)
			}
		}
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("registry: can not find local ip")
	} else if len(ips) > 1 {
		// by default, return the first network IP address found.
		ipGrpc = ips[0].String()

		grpcNet := os.Getenv("DSB_GRPC_NETWORK")
		_, ipNetGrpc, err := net.ParseCIDR(grpcNet)
		if err != nil {
			c.logger.Error("An invalid network CIDR is set in environment DSB_HOTELRESERV_GRPC_NETWORK: %v", grpcNet)
		} else {
			for _, ip := range ips {
				if ipNetGrpc.Contains(ip) {
					ipGrpc = ip.String()
					c.logger.Info("gRPC traffic is routed to the dedicated network %s", ipGrpc)
					break
				}
			}
		}
	} else {
		// only one network device existed
		ipGrpc = ips[0].String()
	}

	return ipGrpc, nil
}

// Register a service with registry
func (c *Client) Register(name string, id string, ip string, port int) error {
	if ip == "" {
		var err error
		ip, err = c.getLocalIP()
		if err != nil {
			return err
		}
	}
	reg := &consul.AgentServiceRegistration{
		ID:      id,
		Name:    name,
		Port:    port,
		Address: ip,
	}
	c.logger.Info("Trying to register service [ name: %s, id: %s, address: %s:%d ]", name, id, ip, port)
	return c.Agent().ServiceRegister(reg)
}

// Deregister removes the service address from registry
func (c *Client) Deregister(id string) error {
	return c.Agent().ServiceDeregister(id)
}
