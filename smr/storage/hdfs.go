package storage

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"net"
	"strings"
	"time"
)

const (
	defaultHdfsUsername string = "jovyan"
)

// HdfsProvider implements the Provider API for redis.
type HdfsProvider struct {
	*baseProvider

	hdfsUsername string
	hdfsClient   *hdfs.Client
}

func NewHdfsProvider(hostname string, deploymentMode string) *HdfsProvider {
	baseProvider := newBaseProvider(hostname, deploymentMode)

	provider := &HdfsProvider{
		baseProvider: baseProvider,
		hdfsUsername: defaultHdfsUsername,
	}

	return provider
}

// SetHdfsUsername sets the username to use when connecting to HDFS.
//
// If the HdfsProvider is already connected to HDFS, then changing the username will not have an effect
// unless the HdfsProvider reconnects to HDFS.
func (p *HdfsProvider) SetHdfsUsername(user string) {
	p.hdfsUsername = user
}

func (p *HdfsProvider) Connect() error {
	p.sugaredLogger.Debug("Connecting to remote storage",
		zap.String("remote_storage", "hdfs"),
		zap.String("hostname", p.hostname))

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{p.hostname},
		User:      p.hdfsUsername,
		NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext(ctx, network, address)
			if err != nil {
				p.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' with network '%s' because: %v", address, network, err)
				return nil, err
			}
			return conn, nil
		},
		// Temporary work-around to deal with Kubernetes networking issues with HDFS.
		// The HDFS NameNode returns the IP for the client to use to connect to the DataNode for reading/writing file blocks.
		// At least for development/testing, I am using a local Kubernetes cluster and a local HDFS deployment.
		// So, the HDFS NameNode returns the local IP address. But since Kubernetes Pods have their own local host, they cannot use this to connect to the HDFS DataNode.
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			if p.deploymentMode == "LOCAL" || p.deploymentMode == "DOCKER_COMPOSE" {
				// If it is local, then we just use the loopback address (or whatever) to the host,
				// which is where the data node is running.
				originalAddress := address
				dataNodeAddress := strings.Split(p.hostname, ":")[0]
				dataNodePort := strings.Split(address, ":")[1]                // Get the port that the DataNode is using. Discard the IP address.
				address = fmt.Sprintf("%s:%s", dataNodeAddress, dataNodePort) // returns the IP address that will enable the local k8s Pods to find the local DataNode.
				p.logger.Debug("Modified HDFS DataNode address.", zap.String("original_address", originalAddress), zap.String("updated_address", address))
			}

			p.logger.Info("Dialing HDFS DataNode.", zap.String("datanode_address", address))

			childCtx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(childCtx, network, address)
			if err != nil {
				p.sugaredLogger.Errorf("Failed to dial HDFS DataNode at address '%s' because: %v", address, err)
				return nil, err
			}

			return conn, nil
		},
	})

	if err != nil {
		p.logger.Error("Failed to create HDFS client.", zap.String("remote_storage_hostname", p.hostname), zap.Error(err))
		return err
	} else {
		p.sugaredLogger.Infof("Successfully connected to HDFS at '%s'", p.hostname)
		fmt.Printf("Successfully connected to HDFS at '%s'\n", p.hostname)
		p.hdfsClient = hdfsClient
	}

	return nil
}
