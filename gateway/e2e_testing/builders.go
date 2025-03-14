package e2e_testing

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	gatewayDaemon "github.com/scusemua/distributed-notebook/gateway/daemon"
	gatewayDomain "github.com/scusemua/distributed-notebook/gateway/domain"
	schedulerDaemon "github.com/scusemua/distributed-notebook/local_daemon/daemon"
	schedulerDomain "github.com/scusemua/distributed-notebook/local_daemon/domain"
	"log"
	"os"
	"sync"
)

type GatewayBuilder struct {
	ClusterGatewayOptions *gatewayDomain.ClusterGatewayOptions

	ConfigFunc func(opts *gatewayDomain.ClusterGatewayOptions)
}

func NewGatewayBuilder(schedulingPolicy scheduling.PolicyKey) *GatewayBuilder {
	builder := &GatewayBuilder{}

	if schedulingPolicy == scheduling.Static {
		err := json.Unmarshal([]byte(gatewayStaticOptsJson), &builder.ClusterGatewayOptions)
		if err != nil {
			panic(err)
		}
	} else {
		panic(fmt.Sprintf("Unsupported scheduling policy: \"%s\"", schedulingPolicy.String()))
	}

	builder.ClusterGatewayOptions.PrewarmingEnabled = false
	builder.ClusterGatewayOptions.IdleSessionReclamationEnabled = false

	return builder
}

func (b *GatewayBuilder) WithPrewarming() *GatewayBuilder {
	b.ClusterGatewayOptions.PrewarmingEnabled = true
	return b
}

func (b *GatewayBuilder) WithoutPrewarming() *GatewayBuilder {
	b.ClusterGatewayOptions.PrewarmingEnabled = false
	return b
}

func (b *GatewayBuilder) WithDebugLogging() *GatewayBuilder {
	b.ClusterGatewayOptions.Verbose = true
	b.ClusterGatewayOptions.Debug = true
	return b
}

func (b *GatewayBuilder) WithoutDebugLogging() *GatewayBuilder {
	b.ClusterGatewayOptions.Verbose = false
	b.ClusterGatewayOptions.Debug = false
	return b
}

func (b *GatewayBuilder) WithPort(port int) *GatewayBuilder {
	b.ClusterGatewayOptions.JupyterGrpcPort = port
	return b
}

func (b *GatewayBuilder) WithProvisionerPort(provisionerPort int) *GatewayBuilder {
	b.ClusterGatewayOptions.ProvisionerPort = provisionerPort
	return b
}

func (b *GatewayBuilder) WithJupyterPort(jupyterPort int) *GatewayBuilder {
	b.ClusterGatewayOptions.JupyterGrpcPort = jupyterPort
	return b
}

func (b *GatewayBuilder) WithInitialClusterSize(size int) *GatewayBuilder {
	b.ClusterGatewayOptions.InitialClusterSize = size
	return b
}

func (b *GatewayBuilder) WithIdleSessionReclamation() *GatewayBuilder {
	b.ClusterGatewayOptions.IdleSessionReclamationEnabled = true
	return b
}

func (b *GatewayBuilder) WithoutIdleSessionReclamation() *GatewayBuilder {
	b.ClusterGatewayOptions.IdleSessionReclamationEnabled = false
	return b
}

func (b *GatewayBuilder) WithConnectionInfo(connInfo *jupyter.ConnectionInfo) *GatewayBuilder {
	b.ClusterGatewayOptions.ConnectionInfo = *connInfo
	return b
}

func (b *GatewayBuilder) WithConfigFunc(confFunc func(opts *gatewayDomain.ClusterGatewayOptions)) *GatewayBuilder {
	b.ConfigFunc = confFunc
	return b
}

func (b *GatewayBuilder) Build() *gatewayDaemon.ClusterGatewayImpl {
	var clusterGatewayDone sync.WaitGroup
	clusterGatewaySig := make(chan os.Signal, 1)
	clusterGatewayFinalize := func(fix bool, identity string, distributedCluster *gatewayDaemon.DistributedCluster) {
		log.Printf("[WARNING] cluster Gateway's finalizer called with fix=%v and identity=\"%s\"\n", fix, identity)
	}

	if b.ConfigFunc != nil {
		b.ConfigFunc(b.ClusterGatewayOptions)
	}

	clusterGateway, _ := gatewayDaemon.CreateAndStartClusterGatewayComponents(b.ClusterGatewayOptions,
		&clusterGatewayDone, clusterGatewayFinalize, clusterGatewaySig)

	if clusterGateway == nil {
		panic("Failed to create cluster Gateway")
	}

	return clusterGateway
}

type LocalSchedulerBuilder struct {
	LocalDaemonOptions *schedulerDomain.LocalDaemonOptions
	ConfigFunc         func(opts *schedulerDomain.LocalDaemonOptions)
}

func NewLocalSchedulerBuilder(schedulingPolicy scheduling.PolicyKey) *LocalSchedulerBuilder {
	builder := &LocalSchedulerBuilder{}

	if schedulingPolicy == scheduling.Static {
		err := json.Unmarshal([]byte(localSchedulerStaticOptsJson), &builder.LocalDaemonOptions)
		if err != nil {
			panic(err)
		}
	} else {
		panic(fmt.Sprintf("Unsupported scheduling policy: \"%s\"", schedulingPolicy.String()))
	}

	return builder
}

func (b *LocalSchedulerBuilder) WithGrpcPort(port int) *LocalSchedulerBuilder {
	b.LocalDaemonOptions.Port = port
	return b
}

func (b *LocalSchedulerBuilder) WithProvisionerAddress(provisionerAddress string) *LocalSchedulerBuilder {
	b.LocalDaemonOptions.ProvisionerAddr = provisionerAddress
	return b
}

func (b *LocalSchedulerBuilder) WithConnInfo(connInfo *jupyter.ConnectionInfo) *LocalSchedulerBuilder {
	b.LocalDaemonOptions.ConnectionInfo = *connInfo
	return b
}

func (b *LocalSchedulerBuilder) WithDebugLogging() *LocalSchedulerBuilder {
	b.LocalDaemonOptions.Verbose = true
	b.LocalDaemonOptions.Debug = true
	return b
}

func (b *LocalSchedulerBuilder) WithoutDebugLogging() *LocalSchedulerBuilder {
	b.LocalDaemonOptions.Verbose = false
	b.LocalDaemonOptions.Debug = false
	return b
}

func (b *LocalSchedulerBuilder) WithConfigFunc(confFunc func(opts *schedulerDomain.LocalDaemonOptions)) *LocalSchedulerBuilder {
	b.ConfigFunc = confFunc
	return b
}

func (b *LocalSchedulerBuilder) Build() (localScheduler *schedulerDaemon.LocalScheduler, closeConnections func()) {
	localDaemonFinalize := func(fix bool) {
		log.Printf("[WARNING] Local Daemon 1's finalizer called with fix=%v\n", fix)
	}

	var localDaemonDone sync.WaitGroup
	localDaemonSig := make(chan os.Signal, 1)

	if b.ConfigFunc != nil {
		b.ConfigFunc(b.LocalDaemonOptions)
	}

	return schedulerDaemon.CreateAndStartLocalDaemonComponents(b.LocalDaemonOptions,
		&localDaemonDone, localDaemonFinalize, localDaemonSig)
}

func GetConnectionInfo(basePort int) *jupyter.ConnectionInfo {
	return &jupyter.ConnectionInfo{
		Transport:        "tcp",
		Key:              uuid.NewString(),
		SignatureScheme:  messaging.JupyterSignatureScheme,
		IP:               "127.0.0.1",
		ControlPort:      basePort,
		ShellPort:        basePort + 1,
		StdinPort:        basePort + 2,
		HBPort:           basePort + 3,
		IOPubPort:        basePort + 4,
		IOSubPort:        basePort + 5,
		AckPort:          basePort + 6,
		NumResourcePorts: 128,
	}
}
