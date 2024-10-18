#!/bin/bash

# Generate mocks for gRPC
mockgen -source ./common/proto/gateway_grpc.pb.go -package mock_proto -destination "./common/mock_proto/mock_proto_api.go"

# Generate mocks for the KernelClient / KernelReplicaClient
mockgen -source common/jupyter/client/types.go -package mock_client -destination "/home/scusemua/go/pkg/distributed-notebook/common/jupyter/mock_client/mock_client.go"

# Generate mocks for the DistributedKernelClient
mockgen -source common/jupyter/client/types.go -package mock_client -destination "/home/scusemua/go/pkg/distributed-notebook/common/jupyter/mock_client/mock_client.go"

# Generate mocks for the scheduling.
mockgen -source common/scheduling/cluster.go -package mock_scheduling -destination "/home/scusemua/go/pkg/distributed-notebook/common/mock_scheduling/mock_cluster.go"