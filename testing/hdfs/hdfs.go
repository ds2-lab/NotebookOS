package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/colinmarc/hdfs/v2"
)

const (
	address string = "host.docker.internal"
	port    int    = 10000
)

func main() {
	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{fmt.Sprintf("%s:%d", address, port)},
		User:      "jovyan",
		NamenodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(ctx, network, address)
			if err != nil {
				fmt.Printf("[ERROR] Failed to dial HDFS DataNode at address '%s' with network '%s' because: %v\n", address, network, err)
				return nil, err
			}
			return conn, nil
		},
		// Temporary work-around to deal with Kubernetes networking issues with HDFS.
		// The HDFS NameNode returns the IP for the client to use to connect to the DataNode for reading/writing file blocks.
		// At least for development/testing, I am using a local Kubernetes cluster and a local HDFS deployment.
		// So, the HDFS NameNode returns the local IP address. But since Kubernetes Pods have their own local host, they cannot use this to connect to the HDFS DataNode.
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			// If we stop using a 'local' HDFS deployment, then we may want another IP address (not the default gateway).
			// gateway, err := gateway.DiscoverGateway()
			// if err != nil {
			// 	fmt.Printf("[ERROR] Failed to resolve default gateway adderss while dialing HDFS DataNode: %v\n", err)
			// 	return nil, err
			// } else {
			// 	fmt.Printf("Discovered default gateway address while dialing HDFS DataNode: %s\n", gateway.String())
			// }

			//port := strings.Split(address, ":")[1]                       // Get the port that the DataNode is using. Discard the IP address.
			//modified_address := fmt.Sprintf("%s:%s", "172.18.0.1", port) // Return the IP address that will enable the local k8s Pods to find the local DataNode.
			fmt.Printf("Dialing HDFS DataNode. Address: '%s'\n", address)

			childCtx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			conn, err := (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext(childCtx, network, address)
			if err != nil {
				fmt.Printf("[ERROR] Failed to dial HDFS DataNode at address '%s' because: %v\n", address, err)
				return nil, err
			}

			return conn, nil
		},
	})

	if err != nil {
		fmt.Printf("[ERROR] Failed to connect to HDFS because: %v\n", err)
		return
	} else {
		fmt.Printf("Successfully connected to HDFS: %v\n", hdfsClient.Name())
	}

	fileInfos, err := hdfsClient.ReadDir("/")
	if err != nil {
		fmt.Printf("[ERROR] Failed to list contents of \"/\" directory: %v\n", err)
		return
	}

	if len(fileInfos) == 0 {
		err = hdfsClient.Mkdir("/foo", 0x777)
		if err != nil {
			fmt.Printf("[ERROR] Failed to create directory \"/foo\": %v\n", err)
			return
		} else {
			fmt.Printf("Successfully created directory \"/foo\"\n")
		}
	}

	fmt.Printf("Contents of directory \"/\" (%d):\n", len(fileInfos))
	for _, fileInfo := range fileInfos {
		fmt.Printf("\t%v\n", fileInfo)
	}
}
