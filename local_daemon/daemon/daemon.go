package daemon

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/petermattis/goid"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"

	"github.com/zhangjyr/distributed-notebook/local_daemon/device"
	"github.com/zhangjyr/distributed-notebook/local_daemon/invoker"
)

const (
	ShellExecuteReply      = "execute_reply"
	ShellExecuteRequest    = "execute_request"
	ShellYieldExecute      = "yield_execute"
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"

	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/kernel-configmap"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"

	// Passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg = "target_replica"

	YieldInsufficientGPUs         YieldReason = "YieldInsufficientGPUs"         // Yield because there are not enough GPUs available.
	YieldDifferentReplicaTargeted YieldReason = "YieldDifferentReplicaTargeted" // Yield because another replica was explicitly targeted.
)

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")
	ErrNoHandler        = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in SchedulerDaemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")
	ErrRequestFailed    = status.Errorf(codes.DeadlineExceeded, "could not complete kernel request in-time")

	// Internal errors
	ErrHeaderNotFound                        = errors.New("message header not found")
	ErrKernelNotFound                        = errors.New("kernel not found")
	ErrKernelNotReady                        = errors.New("kernel not ready")
	ErrKernelIDRequired                      = errors.New("kernel id frame is required for kernel_info_request")
	ErrUnexpectedZMQMessageType              = errors.New("received ZMQ message of unexpected type")
	ErrKernelRegistrationNotificationFailure = errors.New("could not notify gateway of kernel registration")

	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute
)

type YieldReason string
type SchedulerDaemonConfig func(*SchedulerDaemon)

type SchedulerDaemonOptions struct {
	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer     bool   `name:"direct" description:"True if the scheduler serves jupyter notebook directly."`
	SMRPort          int    `name:"smr_port" description:"Port used by the SMR protocol."`
	NumGPUs          int64  `name:"max-actual-gpu-per-node" json:"max-actual-gpu-per-node" yaml:"max-actual-gpu-per-node" description:"The total number of GPUs that should be available on each node."`
	SchedulingPolicy string `name:"scheduling-policy" description:"The scheduling policy to use. Options are 'default, 'static', and 'dynamic'."`
	DeploymentMode   string `name:"deployment_mode" description:"Options are 'docker' and 'kubernetes'."`
}

func (o SchedulerDaemonOptions) String() string {
	return fmt.Sprintf("DirectServer: %v", o.DirectServer)
}

// SchedulerDaemon is the daemon that proxy requests to kernel replicas on local-host.
//
// WIP: Replica membership change.
// TODO: Distinguish reachable host list from replica list.
// TODO: Synchronize resource status using replica network (e.g., control socket). Synchoronization message should load-balance between replicas mapped the same host.
type SchedulerDaemon struct {
	// Options
	id       string
	nodeName string

	virtualGpuPluginServer device.VirtualGpuPluginServer

	schedulingPolicy string
	gateway.UnimplementedLocalGatewayServer
	router    *router.Router
	scheduler core.LocalDaemonClient

	// Options
	connectionOptions      *jupyter.ConnectionInfo
	schedulerDaemonOptions SchedulerDaemonOptions

	// Cluster client
	Provisioner gateway.ClusterGatewayClient

	smrPort int

	// members
	transport string
	ip        string
	kernels   hashmap.HashMap[string, client.KernelReplicaClient]
	log       logger.Logger

	// The IOPub socket that the Gateway subscribes to.
	// All pub/sub messages are forwarded from kernels to the gateway (througth us, the local daemon) using this socket.
	// We wrap the messages in another message that just has a header that is the kernel ID.
	// This enables the Gateway's SUB sockets to filter messages from each kernel.
	// iopub *jupyter.Socket

	// devicePluginServer deviceplugin.VirtualGpuPluginServer

	// There's a simple TCP server that listens for kernel registration notifications on this port.
	kernelRegistryPort int

	availablePorts *utils.AvailablePorts

	// Manages "actual" GPU allocations.
	gpuManager *GpuManager

	// "Local mode" is a sort of debug mode where we're running locally rather than in Kubernetes.
	// This will later be replaced by Docker mode, which was the original way of deploying this system.
	localMode bool

	// Kubernetes or Docker.
	deploymentMode types.DeploymentMode

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

type KernelRegistrationPayload struct {
	SignatureScheme string              `json:"signature_scheme"`
	Key             string              `json:"key"`
	Kernel          *gateway.KernelSpec `json:"kernel,omitempty"`
	// ResourceSpec    *gateway.ResourceSpec `json:"resourceSpec,omitempty"`
	ReplicaId      int32                   `json:"replicaId,omitempty"`
	NumReplicas    int32                   `json:"numReplicas,omitempty"`
	Join           bool                    `json:"join,omitempty"`
	PersistentId   *string                 `json:"persistentId,omitempty"`
	PodName        string                  `json:"podName,omitempty"`
	NodeName       string                  `json:"nodeName,omitempty"`
	Cpu            int32                   `json:"cpu,omitempty"`
	Memory         int32                   `json:"memory,omitempty"`
	Gpu            int32                   `json:"gpu,omitempty"`
	ConnectionInfo *jupyter.ConnectionInfo `json:"connection-info,omitempty"`
}

// Incoming connection from local distributed kernel.
type KernelRegistrationClient struct {
	conn net.Conn
}

func New(connectionOptions *jupyter.ConnectionInfo, schedulerDaemonOptions *SchedulerDaemonOptions, kernelRegistryPort int, virtualGpuPluginServer device.VirtualGpuPluginServer, nodeName string, localMode bool, configs ...SchedulerDaemonConfig) *SchedulerDaemon {
	ip := os.Getenv("POD_IP")
	daemon := &SchedulerDaemon{
		connectionOptions:      connectionOptions,
		transport:              "tcp",
		ip:                     ip,
		nodeName:               nodeName,
		kernels:                hashmap.NewCornelkMap[string, client.KernelReplicaClient](1000),
		availablePorts:         utils.NewAvailablePorts(connectionOptions.StartingResourcePort, connectionOptions.NumResourcePorts, 2),
		closed:                 make(chan struct{}),
		cleaned:                make(chan struct{}),
		kernelRegistryPort:     kernelRegistryPort,
		smrPort:                schedulerDaemonOptions.SMRPort,
		gpuManager:             NewGpuManager(schedulerDaemonOptions.NumGPUs),
		virtualGpuPluginServer: virtualGpuPluginServer,
		localMode:              localMode,
	}
	for _, config := range configs {
		config(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon, fmt.Sprintf("LocalDaemon_%s", nodeName), true)
	daemon.scheduler = NewMembershipScheduler(daemon)

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	switch schedulerDaemonOptions.SchedulingPolicy {
	case "default":
		{
			daemon.schedulingPolicy = "default"
			daemon.log.Debug("Using the 'DEFAULT' scheduling policy.")
		}
	case "static":
		{
			daemon.schedulingPolicy = "static"
			daemon.log.Debug("Using the 'STATIC' scheduling policy.")
		}
	case "dynamic-v3":
		{
			daemon.schedulingPolicy = "dynamic-v3"
			daemon.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	case "dynamic-v4":
		{
			daemon.schedulingPolicy = "dynamic-v4"
			daemon.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", schedulerDaemonOptions.SchedulingPolicy))
		}
	}

	switch schedulerDaemonOptions.DeploymentMode {
	case "":
		{
			daemon.log.Info("No 'deployment_mode' specified. Running in default mode: KUBERNETES mode.")
			daemon.deploymentMode = types.KubernetesMode
		}
	case "docker":
		{
			daemon.log.Info("Running in DOCKER mode.")
			daemon.deploymentMode = types.DockerMode
		}
	case "kubernetes":
		{
			daemon.log.Info("Running in KUBERNETES mode.")
			daemon.deploymentMode = types.KubernetesMode
		}
	default:
		{
			panic(fmt.Sprintf("Unknown/unsupported deployment mode: \"%s\"", schedulerDaemonOptions.DeploymentMode))
		}
	}

	daemon.log.Debug("Connection options: %v", daemon.connectionOptions)

	if !localMode && len(nodeName) == 0 {
		panic("Node name is empty.")
	}

	if localMode && len(nodeName) == 0 {
		daemon.nodeName = "LocalNode"
	}

	return daemon
}

// SetID sets the SchedulerDaemon id by the gateway.
func (d *SchedulerDaemon) SetID(ctx context.Context, in *gateway.HostId) (*gateway.HostId, error) {
	// If id has been set(e.g., restored after restart), return the original id.
	if d.id != "" {
		return &gateway.HostId{
			Id:       d.id,
			NodeName: d.nodeName,
		}, nil
	}

	d.id = in.Id
	in.NodeName = d.nodeName
	return in, nil
}

// StartKernel starts a single kernel.
func (d *SchedulerDaemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	return d.StartKernelReplica(ctx, &gateway.KernelReplicaSpec{
		ReplicaId: 1,
		Replicas:  nil,
		Kernel:    in,
	})
}

// Register a Kernel that has started running on the same node as we are.
// This method must be thread-safe.
func (d *SchedulerDaemon) registerKernelReplica(ctx context.Context, kernelRegistrationClient *KernelRegistrationClient) {
	d.log.Debug("Registering Kernel at (remote) address %v", kernelRegistrationClient.conn.RemoteAddr())

	remote_ip, _, err := net.SplitHostPort(kernelRegistrationClient.conn.RemoteAddr().String())
	if err != nil {
		d.log.Error("Failed to extract remote address from kernel registration connection: %v", err)
		d.log.Error("Cannot register kernel.") // TODO(Ben): Handle this more elegantly.
		return
	}

	var registrationPayload KernelRegistrationPayload
	jsonDecoder := json.NewDecoder(kernelRegistrationClient.conn)
	err = jsonDecoder.Decode(&registrationPayload)
	if err != nil {
		d.log.Error("Failed to decode registration payload: %v", err)
		d.log.Error("Cannot register kernel.") // TODO(Ben): Handle this more elegantly.
		return
	}

	d.log.Debug("Received registration payload: %v", registrationPayload)

	invoker := invoker.NewDockerInvoker(d.connectionOptions)

	var connInfo *jupyter.ConnectionInfo
	if d.localMode {
		connInfo = registrationPayload.ConnectionInfo
	} else {
		connInfo = &jupyter.ConnectionInfo{
			IP:              remote_ip,
			Transport:       "tcp",
			ControlPort:     d.connectionOptions.ControlPort,
			ShellPort:       d.connectionOptions.ShellPort,
			StdinPort:       d.connectionOptions.StdinPort,
			HBPort:          d.connectionOptions.HBPort,
			IOSubPort:       d.connectionOptions.IOSubPort,
			IOPubPort:       d.connectionOptions.IOPubPort,
			SignatureScheme: registrationPayload.SignatureScheme,
			Key:             registrationPayload.Key,
		}
	}

	d.log.Debug("connInfo: %v", connInfo)

	kernelReplicaSpec := &gateway.KernelReplicaSpec{
		Kernel:       registrationPayload.Kernel,
		ReplicaId:    registrationPayload.ReplicaId,   // Can get (from config file).
		NumReplicas:  registrationPayload.NumReplicas, // Can get (from config file).
		Join:         registrationPayload.Join,        // Can get (from config file).
		PersistentId: registrationPayload.PersistentId,
	}

	d.log.Debug("Kernel replica spec: %v", kernelReplicaSpec)

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		panic(err)
	}

	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, invoker)
	// We're passing "" for the persistent ID here; we'll re-assign it once we receive the persistent ID from the Cluster Gateway.
	kernel := client.NewKernelClient(kernelCtx, kernelReplicaSpec, connInfo, true, listenPorts[0], listenPorts[1], registrationPayload.PodName, registrationPayload.NodeName, d.smrReadyCallback, d.smrNodeAddedCallback, "", d.id, false)
	shell := d.router.Socket(jupyter.ShellMessage)
	if d.schedulerDaemonOptions.DirectServer {
		d.log.Debug("Initializing shell forwarder for kernel \"%s\"", kernelReplicaSpec.Kernel.Id)
		var err error
		shell, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			d.closeKernel(kernel, fmt.Sprintf("failed to initialize shell forwarder for kernel \"%s\". Error: %v", kernelReplicaSpec.Kernel.Id, err))
			return // nil, status.Errorf(codes.Internal, err.Error())
		}
		d.log.Debug("Successfully initialized shell forwarder for kernel \"%s\"", kernelReplicaSpec.Kernel.Id)
	}

	iopub, err := kernel.InitializeIOForwarder()

	if err != nil {
		d.closeKernel(kernel, fmt.Sprintf("failed to initialize io forwarder (IO PUB socket) for kernel \"%s\". Error: %v", kernelReplicaSpec.Kernel.Id, err))
		return // nil, status.Errorf(codes.Internal, err.Error())
	}

	// Though named IOPub, it is a sub socket for a client.
	// Subscribe to all messages.
	// Dial our self if the client is running and serving heartbeat.
	// Try dial, ignore failure.
	// The function will default to `kernelReplicaClientImpl::handleMsg` if the provided handler is null.
	iosub, err := kernel.InitializeIOSub(nil, "")
	if err != nil {
		d.log.Error("Failed to initialize IO SUB socket. Error: %v", err)
		d.closeKernel(kernel, fmt.Sprintf("Failed to initialize IO SUB socket. Error: %v", err))
		return
	}

	if err := kernel.Validate(); err != nil {
		d.closeKernel(kernel, "validation error")
		return // nil, status.Errorf(codes.Internal, err.Error())
	}

	// Handle kernel response.
	kernel.AddIOHandler(jupyter.MessageTypeSMRLeadTask, d.handleSMRLeadTask)

	// Register kernel.
	d.kernels.Store(kernel.ID(), kernel)

	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager(manager.py)
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	// d.log.Debug("iopub.Port: %d", d.iopub.Port)

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(shell.Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(iopub.Port), // TODO(Ben): Are these still needed? I think so...
		IosubPort:       int32(iosub.Port), // TODO(Ben): Are these still needed? I think so...
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}

	kernelRegistrationNotification := &gateway.KernelRegistrationNotification{
		ConnectionInfo: info,
		KernelId:       kernel.ID(),
		SessionId:      "N/A",
		ReplicaId:      registrationPayload.ReplicaId,
		HostId:         d.id,
		KernelIp:       remote_ip,
		PodName:        registrationPayload.PodName,
		NodeName:       registrationPayload.NodeName,
		// ResourceSpec:   registrationPayload.ResourceSpec,
	}

	d.log.Info("Kernel %s registered: %v. Notifying Gateway now.", kernelReplicaSpec.ID(), info)

	num_tries := 0
	max_num_tries := 3

	var response *gateway.KernelRegistrationNotificationResponse
	// TODO: Figure out a better way to handle this. As of right now, we really cannot recover from this.
	for num_tries < max_num_tries {
		response, err = d.Provisioner.NotifyKernelRegistered(ctx, kernelRegistrationNotification)
		if err != nil {
			d.log.Error("Error encountered while notifying Gateway of kernel registration (attempt %d/%d): %s", num_tries+1, max_num_tries, err.Error())
			num_tries += 1

			if num_tries < max_num_tries {
				time.Sleep(time.Second * time.Duration(num_tries))
			}
			continue
		} else if response == nil {
			d.log.Error("Failed to notify Gateway of kernel registration (attempt %d/%d).", num_tries+1, max_num_tries)
			num_tries += 1

			if num_tries < max_num_tries {
				time.Sleep(time.Second * time.Duration(num_tries))
			}
			continue
		}

		break
	}

	if response == nil {
		d.log.Error("Failed to notify Gateway of kernel registration after %d attempts.", max_num_tries)
		panic(ErrKernelRegistrationNotificationFailure)
	}

	d.log.Debug("Successfully notified Gateway of kernel registration. Will be assigning replica ID of %d to kernel. Replicas: %v.", response.Id, response.Replicas)
	d.log.Debug("Resource spec for kernel %s: %v", kernel.ID(), response.ResourceSpec)

	kernel.SetResourceSpec(response.ResourceSpec)
	kernel.SetReplicaID(response.Id)

	payload := map[string]interface{}{
		"smr_node_id": response.Id,
		"hostname":    remote_ip,
		"replicas":    response.Replicas,
	}

	if response.PersistentId != nil && response.GetPersistentId() != "" {
		d.log.Debug("Including persistent store ID \"%s\" in notification response to replica %d of kernel %s.", response.GetPersistentId(), response.Id, kernel.ID())
		payload["persistent_id"] = response.GetPersistentId()
		kernel.SetPersistentID(response.GetPersistentId())
		// hdfsDataDirectoryExpected = true
	} else {
		d.log.Debug("No persistent ID to include in response.")
	}

	if response.DataDirectory != nil && response.GetDataDirectory() != "" {
		d.log.Debug("Including data directory \"%s\" in notification response to replica %d of kernel %s.", response.GetDataDirectory(), response.Id, kernel.ID())
		payload["data_directory"] = response.GetDataDirectory()
	} else {
		d.log.Debug("No data directory to include in response.")
	}

	payload_json, err := json.Marshal(payload)
	if err != nil {
		d.log.Error("Error encountered while marshalling replica ID to JSON: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		panic(err)
	}

	bytes_written, err := kernelRegistrationClient.conn.Write(payload_json)
	if err != nil {
		d.log.Error("Error encountered while writing replica ID back to kernel: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		panic(err)
	}
	d.log.Debug("Wrote %d bytes back to kernel in response to kernel registration.", bytes_written)

	// TODO(Ben): Need a better system for this. Basically, give the kernel time to setup its persistent store.
	time.Sleep(time.Second * 1)
}

func (d *SchedulerDaemon) AckHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	ack, err := server.ExtractMessageAcknowledgement(msg)
	if err != nil {
		d.log.Error("Failed to extract message acknowledgement from ACK message because: %s", err.Error())
		return err
	}

	d.log.Debug("Received ACK: %v", ack.String())
	return nil
}

func (d *SchedulerDaemon) smrReadyCallback(kernelClient client.KernelReplicaClient) {
	d.log.Debug("Replica %d of kernel %s is ready to join its SMR cluster.", kernelClient.ReplicaID(), kernelClient.ID())

	_, err := d.Provisioner.SmrReady(context.TODO(), &gateway.SmrReadyNotification{
		KernelId:     kernelClient.ID(),
		ReplicaId:    kernelClient.ReplicaID(),
		PersistentId: kernelClient.PersistentID(),
		Address:      kernelClient.Address(),
	})

	if err != nil {
		d.log.Error("Error when informing Gateway of SMR-Ready for replica %d of kernel %s: %v", kernelClient.ReplicaID(), kernelClient.ID(), err)
	}
}

func (d *SchedulerDaemon) GetActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.GpuInfo, error) {
	gpuInfo := &gateway.GpuInfo{
		SpecGPUs:              int32(d.gpuManager.specGPUs.InexactFloat64()),
		IdleGPUs:              int32(d.gpuManager.idleGPUs.InexactFloat64()),
		CommittedGPUs:         int32(d.gpuManager.committedGPUs.InexactFloat64()),
		PendingGPUs:           int32(d.gpuManager.pendingGPUs.InexactFloat64()),
		NumPendingAllocations: int32(d.gpuManager.NumAllocations()),
		NumAllocations:        int32(d.gpuManager.NumPendingAllocations()),
		GpuSchedulerID:        d.gpuManager.id,
		LocalDaemonID:         d.id,
	}

	// d.log.Debug("Returning GPU information: %v", gpuInfo)

	return gpuInfo, nil
}

func (d *SchedulerDaemon) PrepareToMigrate(ctx context.Context, req *gateway.ReplicaInfo) (*gateway.PrepareToMigrateResponse, error) {
	kernelId := req.KernelId
	replicaId := req.ReplicaId
	d.log.Debug("Preparing to migrate replica %d of kernel %s now.", req.ReplicaId, req.KernelId)

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernelReplicaClientImpl for kernel %s.", kernelId)
		return nil, ErrInvalidParameter
	}

	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypePrepareToMigrateRequest, kernel.Sessions()[0])
	frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: kernel.Address(),
	})
	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for prepare-to-migrate request to replica %d of kernel %s: %v", replicaId, kernelId, err)
		return nil, err
	}

	d.log.Debug("Sending Jupyter 'prepare-to-migrate' request to replica %d of kernel %s now.", req.ReplicaId, req.KernelId)
	msg := &zmq4.Msg{Frames: frames}
	var requestWG sync.WaitGroup
	requestWG.Add(1)
	var dataDirectory string

	err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, msg, func(kernel core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
		d.log.Debug("Received response from 'prepare-to-migrate' request.")

		for i, frame := range msg.Frames {
			d.log.Debug("Frame #%d: %s", i, string(frame))
		}

		var frames jupyter.JupyterFrames = msg.Frames
		var respMessage jupyter.MessageDataDirectory
		if err := frames.Validate(); err != nil {
			d.log.Error("Failed to validate frames of `MessageDataDirectory` message: %v", err)
			return err
		}
		err := frames.DecodeContent(&respMessage)
		if err != nil {
			d.log.Error("Failed to decode Content frame of `MessageDataDirectory` message: %v", err)
			return err
		}

		// It seems like this particular message is sent in the 6th frame, which is the Buffers frame, rather than the Content frame.
		if respMessage.NodeID == 0 {
			err := frames.DecodeBuffers(&respMessage)

			if err != nil {
				d.log.Error("Failed to decode Buffer frame of `MessageDataDirectory` message: %v", err)
				return err
			}
		}

		dataDirectory = respMessage.DataDirectory
		if respMessage.Status == "error" {
			var msgErr jupyter.MessageError
			frames.DecodeBuffers(&msgErr)

			d.log.Error("Error encountered by kernel %s while it was preparing to migrate: %v", kernel.ID(), msgErr)
			return fmt.Errorf("ErrPrepareToMigrateFailed (%s) -- %s: %s", msgErr.Status, msgErr.ErrName, msgErr.ErrValue)
		} else {
			d.log.Debug("Response from 'prepare-to-migrate' request: %s", respMessage.String())
		}

		return nil
	}, requestWG.Done)
	if err != nil {
		d.log.Error("Error occurred while issuing prepare-to-migrate request to replica %d of kernel %s: %v", replicaId, kernelId, err)
		return nil, err
	}

	// TODO: Add a timeout to this operation using context.WithTimeout.
	requestWG.Wait()

	// Because of how requests are handled under the covers, the value of `requestReceived` will necessarily be 1 at this point
	// if we received a response. This is because the handler is called BEFORE Done() is called on the 'requestWG'.
	d.log.Debug("Prepare-to-migrate request to replica %d of kernel %s succeeded. Data directory: \"%s\"", replicaId, kernelId, dataDirectory)

	if dataDirectory == "" {
		d.log.Error("Data directory is still the empty string despite the request completing successfully.")
	}

	return &gateway.PrepareToMigrateResponse{
		Id:       replicaId,
		KernelId: kernelId,
		DataDir:  dataDirectory,
	}, nil
}

// Ensure that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (d *SchedulerDaemon) YieldNextExecution(ctx context.Context, in *gateway.KernelId) (*gateway.Void, error) {
	kernelId := in.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernelReplicaClientImpl for kernel %s specified in 'YieldNextExecution' request.", kernelId)
		return gateway.VOID, ErrInvalidParameter
	}

	d.log.Debug("Kernel %s will YIELD its next execution request.", in.Id)

	kernel.YieldNextExecutionRequest()
	return gateway.VOID, nil
}

func (d *SchedulerDaemon) UpdateReplicaAddr(ctx context.Context, req *gateway.ReplicaInfoWithAddr) (*gateway.Void, error) {
	kernelId := req.KernelId
	hostname := req.Hostname // The new hostname of the replica.
	replicaId := req.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernelReplicaClientImpl for kernel %s.", kernelId)
		return gateway.VOID, ErrInvalidParameter
	}

	d.log.Debug("Informing replicas of kernel %s to update address of replica %d to %s.", kernelId, replicaId, hostname)
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeUpdateReplicaRequest, kernel.Sessions()[0])
	frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: hostname,
	})
	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for update-replica request to kernel %s: %v", kernelId, err)
		return gateway.VOID, err
	}

	msg := &zmq4.Msg{Frames: frames}

	var currentNumTries int = 0
	var maxNumTries int = 3
	var success bool = false
	for currentNumTries < maxNumTries {
		var wg sync.WaitGroup
		var requestReceived int32 = 0
		wg.Add(1)
		err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, msg, func(kernel core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
			d.log.Debug("Received response from 'update-replica' request.")

			// Record that we've received the response.
			swapped := atomic.CompareAndSwapInt32(&requestReceived, 0, 1)
			if !swapped {
				d.log.Warn("Apparently we've already received a request for prepare-to-migrate request sent to replica %d of kernel %s: %v", replicaId, kernelId)
			}

			return nil
		}, wg.Done)
		if err != nil {
			d.log.Error("Error occurred while issuing update-replica request to kernel %s: %v", kernelId, err)
			return gateway.VOID, err
		}
		wg.Wait()

		// Because of how requests are handled under the covers, the value of `requestReceived` will necessarily be 1 at this point
		// if we received a response. This is because the handler is called BEFORE Done() is called on the 'requestWG'.
		if atomic.LoadInt32(&requestReceived) == 0 {
			d.log.Error("TIMED-OUT: 'Update-replica' request to replica %d of kernel %s did not complete in time.", replicaId, kernelId)
			currentNumTries++
			sleepAmount := time.Millisecond * 1500 * time.Duration(currentNumTries)
			d.log.Error("Sleeping for %d ms before trying again.", sleepAmount)
			time.Sleep(sleepAmount)
			wg.Add(1)
		} else {
			success = true
			break
		}
	}

	if !success {
		return gateway.VOID, ErrRequestFailed
	}

	return gateway.VOID, nil
}

func (d *SchedulerDaemon) AddReplica(ctx context.Context, req *gateway.ReplicaInfoWithAddr) (*gateway.Void, error) {
	kernelId := req.KernelId
	hostname := req.Hostname
	replicaId := req.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernelReplicaClientImpl for kernel %s.", kernelId)
		return gateway.VOID, ErrInvalidParameter
	}

	d.log.Debug("Now that replica %d of kernel %s (host=%s) has been added, notify the existing members.", replicaId, kernelId, hostname)
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeAddReplicaRequest, kernel.Sessions()[0])
	frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: hostname, // s.daemon.getInvoker(kernel).GetReplicaAddress(kernel.KernelSpec(), replicaId),
	})
	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for add-replica request to kernel %s: %v", kernelId, err)
		return gateway.VOID, err
	}

	msg := &zmq4.Msg{Frames: frames}
	var wg sync.WaitGroup
	wg.Add(1)
	err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, msg, nil, wg.Done)
	if err != nil {
		d.log.Error("Error occurred while issuing add-replica request to kernel %s: %v", kernelId, err)
		return gateway.VOID, err
	}
	wg.Wait()

	return gateway.VOID, nil
}

func (d *SchedulerDaemon) smrNodeAddedCallback(readyMessage *jupyter.MessageSMRNodeUpdated) {
	if readyMessage.Success {
		d.log.Debug("Replica %d of kernel %s has successfully joined its SMR cluster.", readyMessage.NodeID, readyMessage.KernelId)
	} else {
		d.log.Error("Replica %d of kernel %s has failed to join its SMR cluster.", readyMessage.NodeID, readyMessage.KernelId)

		// TODO(Ben): Handle this somehow.
		return
	}

	_, err := d.Provisioner.SmrNodeAdded(context.TODO(), &gateway.ReplicaInfo{
		KernelId:     readyMessage.KernelId,
		ReplicaId:    readyMessage.NodeID,
		PersistentId: readyMessage.PersistentID,
	})

	if err != nil {
		// TODO(Ben): Handle this somehow.
		d.log.Error("Error when informing Gateway of SMR Node-Added for replica %d of kernel %s: %v", readyMessage.NodeID, readyMessage.KernelId, err)
	}
}

// Return true if we're running in Docker (i.e., the Docker-based deployment).
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *SchedulerDaemon) DockerMode() bool {
	return d.deploymentMode == types.DockerMode
}

// Return true if we're running in Kubernetes.
func (d *SchedulerDaemon) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// StartKernel launches a new kernel via Docker.
// This is ONLY used in the Docker-based deployment mode.
func (d *SchedulerDaemon) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	if !d.DockerMode() {
		d.log.Error("LocalDaemon cannot explicitly create kernel replica, as we're not running in Docker mode.")
		return nil, types.ErrIncompatibleDeploymentMode
	}

	invoker := invoker.NewDockerInvoker(d.connectionOptions)
	connInfo, err := invoker.InvokeWithContext(ctx, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Initialize kernel client with new context.
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, invoker)

	// kernel := client.NewKernelClient(kernelCtx, in, connInfo)
	listenPorts, err := d.availablePorts.RequestPorts()
	kernel := client.NewKernelClient(kernelCtx, in, connInfo, true, listenPorts[0], listenPorts[1], types.DockerContainerIdTBD, types.DockerNode, d.smrReadyCallback, d.smrNodeAddedCallback, "", d.id, false)

	shell := d.router.Socket(jupyter.ShellMessage)
	if d.schedulerDaemonOptions.DirectServer {
		var err error
		shell, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			d.closeKernel(kernel, "failed initializing shell forwarder")
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	iopub, err := kernel.InitializeIOForwarder()
	if err != nil {
		d.closeKernel(kernel, "failed initializing io forwarder")
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if err := kernel.Validate(); err != nil {
		d.closeKernel(kernel, "validation error")
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Handle kernel response.
	kernel.AddIOHandler(jupyter.MessageTypeSMRLeadTask, d.handleSMRLeadTask)

	// Register kernel.
	d.kernels.Store(kernel.ID(), kernel)
	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager(manager.py)
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(shell.Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(iopub.Port),
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}

	d.log.Info("Kernel %s started: %v", in.ID(), info)
	return info, nil
}

// KernelStatus returns the status of a kernel.
func (d *SchedulerDaemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Warn("Kernel %s not found on query status", in.Id)
		return nil, ErrKernelNotFound
	}

	status, err := d.getInvoker(kernel).Status()
	return d.statusErrorf(kernel, status, err)
}

// KillKernel kills a kernel.
func (d *SchedulerDaemon) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", in.Id)
		return nil, ErrKernelNotFound
	}

	ret = gateway.VOID
	err = d.errorf(d.getInvoker(kernel).Close())
	return
}

// StopKernel stops a kernel.
func (d *SchedulerDaemon) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	d.log.Info("Received instruction to stop kernel %s.", in.Id)
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", in.Id)
		return nil, ErrKernelNotFound
	}

	d.log.Debug("Stopping replica %d of kernel %s now.", kernel.ReplicaID(), in.Id)
	err = d.stopKernel(ctx, kernel, false)
	if err != nil {
		return nil, d.errorf(err)
	}

	d.log.Debug("Successfully stopped replica %d of kernel %s.", kernel.ReplicaID(), in.Id)

	listenPorts := []int{kernel.ShellListenPort(), kernel.IOPubListenPort()}
	err = d.availablePorts.ReturnPorts(listenPorts)
	if err != nil {
		return nil, d.errorf(err)
	}

	return gateway.VOID, nil
}

func (d *SchedulerDaemon) stopKernel(ctx context.Context, kernel client.KernelReplicaClient, ignoreReply bool) (err error) {
	if ignoreReply {
		kernel.AddIOHandler(jupyter.IOTopicShutdown, d.handleIgnoreMsg)
	}

	var msg zmq4.Msg
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeShutdownRequest, kernel.Sessions()[0])
	frames.EncodeContent(&jupyter.MessageShutdownRequest{
		Restart: false,
	})
	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err = kernel.RequestWithHandler(ctx, "Stopping by", jupyter.ControlMessage, &msg, nil, wg.Done)
	if err != nil {
		return err
	}

	d.log.Debug("Sent \"%s\" message to replica %d of kernel %s.", jupyter.MessageTypeShutdownRequest, kernel.ReplicaID(), kernel.ID())

	wg.Wait()

	// d.getInvoker(kernel).Close()
	return nil
}

// WaitKernel waits for a kernel to exit.
func (d *SchedulerDaemon) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", in.Id)
		return nil, ErrKernelNotFound
	}

	status, err := d.getInvoker(kernel).Wait()
	return d.statusErrorf(kernel, status, err)
}

func (d *SchedulerDaemon) SetClose(ctx context.Context, in *gateway.Void) (*gateway.Void, error) {
	d.Close()
	return gateway.VOID, nil
}

func (d *SchedulerDaemon) Start() error {
	d.log.Info("Starting router...")

	// Start cleaning routine.
	go d.cleanUp()

	go d.startKernelRegistryService()

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Shutdown helper routines (e.g., cleanUp())
	close(d.closed)
	return err
}

func (d *SchedulerDaemon) startKernelRegistryService() {
	d.log.Debug("Beginning to listen for kernel registrations at %v", fmt.Sprintf(":%d", d.kernelRegistryPort))

	// Initialize the Kernel Registry listener
	registryListener, err := net.Listen("tcp", fmt.Sprintf(":%d", d.kernelRegistryPort))
	if err != nil {
		log.Fatalf("Failed to listen for kernel registry: %v", err)
	}
	defer registryListener.Close()
	d.log.Info("Scheduler listening for kernel registrations at %v", registryListener.Addr())

	for {
		d.log.Info("Listening for kernel registration connection...")
		conn, err := registryListener.Accept()
		if err != nil {
			d.log.Error("Error encountered while listening for kernel registrations: %v", err)
			continue
		}

		d.log.Info("Accepted kernel registry connection. Local: %s. Remote: %s.", conn.LocalAddr(), conn.RemoteAddr())
		kernelRegistrationClient := &KernelRegistrationClient{conn: conn}
		go d.registerKernelReplica(context.TODO(), kernelRegistrationClient)
	}
}

func (d *SchedulerDaemon) Close() error {
	// Close the router
	d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned
	return nil
}

// RouterProvider implementations.
func (d *SchedulerDaemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	// Kernel ID is not available in the control message.
	_, header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", header.Session)
		return ErrKernelNotFound
	}

	if err := d.forwardRequest(context.Background(), kernel, jupyter.ControlMessage, msg, nil); err != nil {
		return err
	}

	// Handle ShutdownRequest
	if header.MsgType == ShellShutdownRequest {
		go func() {
			status, err := d.getInvoker(kernel).Wait() // Wait() will detect the kernel status and the cleanup() will clean kernel automatically.
			d.statusErrorf(kernel, status, err)
		}()
	}

	return nil
}

func (d *SchedulerDaemon) kernelShellHandler(info core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	return d.ShellHandler(info, msg)
}

func (d *SchedulerDaemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	// d.log.Debug("Received shell message with %d frame(s): %s", len(msg.Frames), msg)
	kernelId, header, offset, err := d.headerAndOffsetFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok && (header.MsgType == ShellKernelInfoRequest || header.MsgType == ShellExecuteRequest) {
		// Register kernel on ShellKernelInfoRequest
		if kernelId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(kernelId)
		if !ok {
			d.log.Error("Could not find kernel with ID %s", kernelId)
			return ErrKernelNotFound
		}

		d.log.Debug("Binding %v with session %s ", kernel, header.Session)
		d.kernels.Store(header.Session, kernel)
		kernel.BindSession(header.Session)
	}
	if kernel == nil {
		d.log.Error("Could not find kernel with ID %s", header.Session)
		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	// d.log.Debug("Shell message with %d frame(s) is targeting replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)

	// TODO(Ben): We'll inspect here to determine if the message is an execute_request.
	// If it is, then we'll see if we have enough resources for the kernel to (potentially) execute the code.
	// If not, we'll change the message's header to "yield_execute".
	// If the message is an execute_request message, then we have some processing to do on it.
	if header.MsgType == ShellExecuteRequest {
		msg = d.processExecuteRequest(msg, kernel, header, offset)
		d.log.Debug("Forwarding shell 'execution-request' with %d frames to replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)
	} else { // Print a message about forwarding generic shell message.
		d.log.Debug("Forwarding shell message with %d frames to replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := kernel.RequestWithHandler(ctx, "Forwarding", jupyter.ShellMessage, msg, d.kernelResponseForwarder, cancel); err != nil {
		return err
	}

	return nil
}

// Deallocate the GPU resources associated with the kernel.
func (d *SchedulerDaemon) processExecuteReply(msg *zmq4.Msg, kernel core.KernelInfo, header *jupyter.MessageHeader) error {
	err := d.gpuManager.ReleaseAllocatedGPUs(kernel.(client.KernelReplicaClient).ReplicaID(), kernel.ID())
	if err != nil {
		d.log.Error("Failed to release GPUs allocated to replica %d of kernel %s because: %v", kernel.(client.KernelReplicaClient).ReplicaID(), kernel.ID(), err)
	}

	return err /* will be nil on success */
}

func (d *SchedulerDaemon) processExecuteRequest(msg *zmq4.Msg, kernel client.KernelReplicaClient, header *jupyter.MessageHeader, offset int) *zmq4.Msg {
	// There may be a particular replica specified to execute the request. We'll extract the ID of that replica to this variable, if it is present.
	var targetReplicaId int32 = -1

	// TODO(Ben): Check GPU resources. If there are sufficiently-many GPUs available, then leave the message as-is.
	// If there are insufficient GPUs available, then we'll modify the message to be a "yield_execute" message.
	// This will force the replica to necessarily yield the execution to the other replicas.
	// If no replicas are able to execute the code due to resource contention, then a new replica will be created dynamically.
	var frames jupyter.JupyterFrames = jupyter.JupyterFrames(msg.Frames)
	var metadataFrame *jupyter.JupyterFrame = frames[offset:].MetadataFrame()
	var metadataDict map[string]interface{}

	d.log.Debug("Processing `execute_request` for kernel %s now.", kernel.ID())

	// Don't try to unmarshal the metadata frame unless the size of the frame is non-zero.
	if len(metadataFrame.Frame()) > 0 {
		// Unmarshal the frame.
		err := metadataFrame.Decode(&metadataDict)
		if err != nil {
			d.log.Error("Error unmarshalling metadata frame for 'execute_request' message: %v", err)
		} else {
			d.log.Debug("Unmarshalled metadata frame for 'execute_request' message: %v", metadataDict)

			// See if there are any notable custom arguments, such as a target replica.
			if val, ok := metadataDict[TargetReplicaArg]; ok {
				targetReplicaAsFloat64, ok := val.(float64)
				if !ok {
					d.log.Error("Could not parse target replica ID in metadata ('%v') for 'execute_request' message: %v", targetReplicaAsFloat64, err)
					targetReplicaId = -1
				} else {
					targetReplicaId = int32(targetReplicaAsFloat64)
					d.log.Debug("Found target replica argument for 'execute_request' message. Target replica: %d.", targetReplicaId)
				}
			}
		}
	} else {
		// If we're not retrieving the map from the message, then we'll create a new map here.
		metadataDict = make(map[string]interface{})
	}

	var (
		// Check if another replica was specified as the one that should execute the code.
		// If this is true, then we'll yield the execution.
		// Note that we may pass 0 to force the execution to fail, for testing/debugging purposes.
		// No SMR replica can have an ID of 0.
		differentTargetReplicaSpecified bool = (targetReplicaId != -1 && targetReplicaId != kernel.ReplicaID())

		// Will store the return value of `AllocatePendingGPUs`. If it is non-nil, then the allocation failed due to insufficient resources.
		err error

		// The number of GPUs required by this kernel replica.
		requiredGPUs decimal.Decimal
	)

	if kernel.ResourceSpec() == nil {
		d.log.Error("Kernel %s (replica %d) does not have a ResourceSpec associated with it...", kernel.ID(), kernel.ReplicaID())
		requiredGPUs = ZeroDecimal.Copy()
	} else {
		d.log.Debug("Kernel %s requires %d GPU(s).", kernel.ID(), kernel.ResourceSpec().GetGpu())
		requiredGPUs = decimal.NewFromFloat(float64(kernel.ResourceSpec().GetGpu()))
	}

	// If the error is non-nil, then there weren't enough idle GPUs available.
	if !differentTargetReplicaSpecified {
		// Only bother trying to allocate GPUs if the request isn't explicitly targeting another replica.
		err = d.gpuManager.AllocatePendingGPUs(requiredGPUs, kernel.ReplicaID(), kernel.ID())
	}

	// Include the current number of idle GPUs availabe.
	idleGPUs := d.gpuManager.IdleGPUs()
	d.log.Debug("Including idle-gpus (%s) in request metadata.", idleGPUs.StringFixed(0))
	metadataDict["idle-gpus"] = idleGPUs

	// There are several circumstances in which we'll need to tell our replica of the target kernel to yield the execution to one of the other replicas:
	// - If there are insufficient GPUs on this node, then our replica will need to yield.
	// - If one of the other replicas was explicitly specified as the target replica, then our replica will need to yield.
	if err != nil || differentTargetReplicaSpecified || kernel.SupposedToYieldNextExecutionRequest() {
		var reason YieldReason
		// Log message depends on which condition was true (first).
		if err != nil {
			d.log.Debug("Insufficient GPUs available (%s) for replica %d of kernel %s to execute code (%v required).", d.gpuManager.IdleGPUs(), kernel.ReplicaID(), kernel.ID(), 0 /* Placeholder */)
			reason = YieldInsufficientGPUs
		} else {
			d.log.Debug("Replica %d of kernel %s is targeted, while we have replica %d running on this node.", targetReplicaId, kernel.ID(), kernel.ReplicaID() /* Placeholder */)
			reason = YieldDifferentReplicaTargeted
		}
		metadataDict["yield-reason"] = reason
		// Convert the message to a yield request.
		// We'll return this converted message, and it'll ultimately be forwarded to the kernel replica in place of the original 'execute_request' message.
		msg, _ = d.convertExecuteRequestToYieldExecute(msg, header, offset)
		frames = msg.Frames

		// If we were told explicitly to YIELD this execution request via the `YieldNextExecutionRequest` API, then record that we've done this.
		// Even if we're yielding for other reasons too, we should still record that we've done this.
		if kernel.SupposedToYieldNextExecutionRequest() {
			kernel.YieldedNextExecutionRequest()
		}
	}

	// Re-encode the metadata frame. It will have the number of idle GPUs available,
	// as well as the reason that the request was yielded (if it was yielded).
	err = frames[offset:].EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		panic(err)
	}

	// Regenerate the signature.
	framesWithoutIdentities, _ := kernel.SkipIdentities(msg.Frames)
	framesWithoutIdentities.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)) // Ignore the error, log it if necessary.

	if verified := d.verifyFrames([]byte(kernel.ConnectionInfo().Key), kernel.ConnectionInfo().SignatureScheme, offset, msg.Frames); !verified {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'", kernel.ConnectionInfo().SignatureScheme, kernel.ConnectionInfo().Key)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", msg)
	}

	return msg
}

func (d *SchedulerDaemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(context.Background(), nil, jupyter.StdinMessage, msg, nil)
}

func (d *SchedulerDaemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(context.Background(), nil, jupyter.HBMessage, msg, nil)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *SchedulerDaemon) kernelIdFromMsg(msg *zmq4.Msg) (id string, sessId bool, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)
	if kernelId != "" {
		return kernelId, false, nil
	}

	header, err := d.headerFromFrames(msg.Frames[offset:])
	if err != nil {
		return "", false, err
	}

	return header.Session, true, nil
}

// Return the current vGPU allocations on this node.
func (d *SchedulerDaemon) GetVirtualGpuAllocations(ctx context.Context, in *gateway.Void) (*gateway.VirtualGpuAllocations, error) {
	allocations := &gateway.VirtualGpuAllocations{
		Allocations: d.virtualGpuPluginServer.GetAllocations(),
	}

	d.log.Debug("Returning vGPU allocations: %v", d.virtualGpuPluginServer.GetAllocations())

	return allocations, nil
}

func (d *SchedulerDaemon) GetVirtualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.VirtualGpuInfo, error) {
	response := &gateway.VirtualGpuInfo{
		TotalVirtualGPUs:     int32(d.virtualGpuPluginServer.NumVirtualGPUs()),
		AllocatedVirtualGPUs: int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()),
		FreeVirtualGPUs:      int32(d.virtualGpuPluginServer.NumFreeVirtualGPUs()),
	}

	d.log.Debug("Returning vGPU information: %v", response)

	return response, nil
}

// Adjust the total number of virtual GPUs available on this node.
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (d *SchedulerDaemon) SetTotalVirtualGPUs(ctx context.Context, in *gateway.SetVirtualGPUsRequest) (*gateway.VirtualGpuInfo, error) {
	newNumVirtualGPUs := in.GetValue()
	if newNumVirtualGPUs < int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()) {
		response := &gateway.VirtualGpuInfo{
			TotalVirtualGPUs:     int32(d.virtualGpuPluginServer.NumVirtualGPUs()),
			AllocatedVirtualGPUs: int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()),
			FreeVirtualGPUs:      int32(d.virtualGpuPluginServer.NumFreeVirtualGPUs()),
		}

		return response, fmt.Errorf("ErrInvalidParameter %w : cannot decrease the total number of vGPUs below the number of allocated vGPUs", ErrInvalidParameter)
	}

	err := d.virtualGpuPluginServer.SetTotalVirtualGPUs(newNumVirtualGPUs)
	if err != nil {
		d.log.Error("Failed to set the total number of virtual GPUs to new value %d: %v", newNumVirtualGPUs, err)
		return nil, err
	}

	response, err := d.GetVirtualGpuInfo(ctx, &gateway.Void{})
	if err != nil {
		d.log.Error("Unexpected error while getting the new virtual GPU info: %v", err)
		return nil, err
	}

	return response, nil
}

// Convert the given message to a "yield_execute" message.
//
// This will return a COPY of the original message with the type field modified to contact "yield_execute" instead of "execute_request".
// On success, the returned error will be nil. If an error occurs, then the returned message will be nil, and the error will be non-nil.
//
// PRECONDITION: The given message must be an "execute_request" message.
// This function will NOT check this. It should be checked before calling this function.
func (d *SchedulerDaemon) convertExecuteRequestToYieldExecute(msg *zmq4.Msg, header *jupyter.MessageHeader, offset int) (*zmq4.Msg, error) {
	d.log.Debug("Converting 'execute_request' message to 'yield_request' message.")

	var err error

	// Clone the original message.
	var newMessage zmq4.Msg = msg.Clone()

	// Change the message header.
	header.MsgType = ShellYieldExecute

	// Create a JupyterFrames struct by wrapping with the message's frames.
	jFrames := jupyter.JupyterFrames(newMessage.Frames)
	if err = jFrames.Validate(); err != nil {
		d.log.Error("Error encountered while converting 'execute_request' message to 'yield_execute' message, specifically while validating the existing frames: %v", err)
		panic(err) // TODO(Ben): Handle this error more gracefully.
		// return nil, err
	}

	// Replace the header with the new header (that has the 'yield_execute' MsgType).
	if jFrames[jupyter.JupyterFrameHeader+offset], err = json.Marshal(header); err != nil {
		d.log.Error("Error encountered while converting 'execute_request' message to 'yield_execute' message, specifically while encoding the new message header: %v", err)
		panic(err) // TODO(Ben): Handle this error more gracefully.
		// return nil, err
	}

	// Replace the frames of the cloned message.
	newMessage.Frames = jFrames

	return &newMessage, nil
}

func (d *SchedulerDaemon) verifyFrames(signkey []byte, signatureScheme string, offset int, frames jupyter.JupyterFrames) bool {
	expect, err := frames.CreateSignature(signatureScheme, signkey, offset)
	if err != nil {
		d.log.Error("Error when creating signature to verify JFrames: %v", err)
		return false
	}

	signature := make([]byte, hex.DecodedLen(len(frames[offset+jupyter.JupyterFrameSignature])))
	hex.Decode(signature, frames[offset+jupyter.JupyterFrameSignature])
	verified := hmac.Equal(expect, signature)

	if !verified {
		d.log.Error("Failed to verify JFrames.\nExpect: '%v'\nSignature: '%v'", expect, signature)
	}

	return verified
}

func (d *SchedulerDaemon) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
	jFrames := jupyter.JupyterFrames(frames)
	if err := jFrames.Validate(); err != nil {
		d.log.Debug("Failed to validate message frames while extracting header.")
		return nil, err
	}

	var header jupyter.MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		d.log.Error("Failed to decode header %v from message frames: %v", string(jFrames[jupyter.JupyterFrameHeader]), err)
		return nil, err
	}

	return &header, nil
}

func (d *SchedulerDaemon) headerFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)

	header, err = d.headerFromFrames(msg.Frames[offset:])

	return kernelId, header, err
}

func (d *SchedulerDaemon) headerAndOffsetFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, offset int, err error) {
	kernelId, _, offset = d.router.ExtractDestFrame(msg.Frames)

	header, err = d.headerFromFrames(msg.Frames[offset:])

	return kernelId, header, offset, err
}

func (d *SchedulerDaemon) kernelFromMsg(msg *zmq4.Msg) (kernel client.KernelReplicaClient, err error) {
	id, _, err := d.kernelIdFromMsg(msg)
	if err != nil {
		return nil, err
	}

	kernel, ok := d.kernels.Load(id)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", id)
		return nil, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, ErrKernelNotReady
	}

	return kernel, nil
}

func (d *SchedulerDaemon) forwardRequest(ctx context.Context, kernel client.KernelReplicaClient, typ jupyter.MessageType, msg *zmq4.Msg, done func()) (err error) {
	goroutineId := goid.Get()
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	d.log.Debug("[gid=%d] Forwarding %v message to replica %d of kernel %s.", goroutineId, typ, kernel.ReplicaID(), kernel.ID())
	if done == nil {
		done = func() {}
	}
	return kernel.RequestWithHandler(ctx, "Forwarding", typ, msg, d.kernelResponseForwarder, done)
}

func (d *SchedulerDaemon) kernelResponseForwarder(from core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	var sender server.Sender

	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)

		// TODO (Ben): If we're forwarding a response back to the Cluster Gateway here, then the Cluster Gateway will ACK the response.
		// We need to:
		// (a) Listen for the ACK.
		// (b) Re-forward the kernel's response to the Cluster Gateway if we don't receive an ACK.
		d.router.RegisterAck(msg)

		// Use the router as the server to forward the kernel's response to the cluster gateway.
		// The socket belongs to the router, hence we use that server.
		sender = d.router
	} else {
		// Use the kernel client as the server to forward the kernel's response to the cluster gateway.
		// The socket belongs to the kernel client's server, hence we use that server.
		sender = from.(client.KernelReplicaClient)
	}

	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if typ == jupyter.ShellMessage {
		_, header, err := d.headerFromMsg(msg)
		if err != nil {
			d.log.Error("Failed to extract header from %v message for kernel %s because: %v", typ, from.ID(), err)
		} else if header.MsgType == ShellExecuteReply {
			d.processExecuteReply(msg, from, header)
		}
	}

	d.log.Debug("Forwarding %v response from %v via %s: %v", typ, from, socket.Name, msg)
	// We should only use the router here if that's where the socket came from...
	go sender.SendMessage(true, socket, "" /* will be auto-resolved */, msg, sender, from.(client.KernelReplicaClient), -1 /* will be auto-resolved */)
	err := socket.Send(*msg)

	if err != nil {
		d.log.Error("Error while forwarding %v response from kernel %s: %s", typ, from.ID(), err.Error())
	} else {
		d.log.Debug("Successfully forwarded %v response from kernel %s.", typ, from.ID())
	}

	return nil // Will be nil on success.
}

func (d *SchedulerDaemon) handleSMRLeadTask(kernel core.Kernel, frames jupyter.JupyterFrames, raw *zmq4.Msg) error {
	messageType, err := frames.GetMessageType()
	if err != nil {
		d.log.Error("Failed to extract message type from SMR Lead ZMQ message: %v", err)
		return err
	}

	if messageType == jupyter.MessageTypeSMRLeadTask {
		var leadMessage jupyter.MessageSMRLeadTask
		if err := frames.DecodeContent(&leadMessage); err != nil {
			d.log.Error("Failed to decode content of SMR Lead ZMQ message: %v", err)
			return err
		}

		client := kernel.(client.KernelReplicaClient)

		d.log.Debug("%v leads the task, GPU required(%v), notify the scheduler.", kernel, leadMessage.GPURequired)
		err := d.gpuManager.AllocateGPUs(decimal.NewFromFloat(client.ResourceSpec().GPU()), client.ReplicaID(), client.ID())
		if err != nil {
			d.log.Error("Could not allocate actual GPUs to replica %d of kernel %s because: %v.", client.ReplicaID(), client.ID(), err)
			panic(err) // TODO(Ben): Handle gracefully.
		}
		go d.scheduler.OnTaskStart(kernel, &leadMessage)
		return jupyter.ErrStopPropagation
	} else if messageType == jupyter.MessageTypeLeadAfterYield {
		// TODO(Ben): Need a better way to propagate errors back to the user, either at the Jupyter Notebook or the Workload Driver.
		panic(fmt.Sprintf("Our replica of kernel %s was selected to lead an execution after explicitly yielding.", kernel.ID()))
	} else {
		d.log.Error("Received message of unexpected type '%s' for SMR Lead ZMQ message topic.", messageType)
		return ErrUnexpectedZMQMessageType
	}
}

func (d *SchedulerDaemon) handleIgnoreMsg(kernel core.Kernel, frames jupyter.JupyterFrames, raw *zmq4.Msg) error {
	d.log.Debug("%v ignores %v", kernel, raw)
	return jupyter.ErrStopPropagation
}

func (d *SchedulerDaemon) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *SchedulerDaemon) statusErrorf(kernel client.KernelReplicaClient, status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}

	if status >= jupyter.KernelStatusExited {
		d.kernels.Delete(kernel.ID())
		for _, session := range kernel.Sessions() {
			d.kernels.Delete(session)
		}
		d.log.Debug("Cleaned kernel %s and associated sessions %v after kernel stopped.", kernel.ID(), kernel.Sessions())
		kernel.Close()
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *SchedulerDaemon) getInvoker(kernel core.Kernel) invoker.KernelInvoker {
	return kernel.Context().Value(ctxKernelInvoker).(invoker.KernelInvoker)
}

func (d *SchedulerDaemon) closeKernel(kernel client.KernelReplicaClient, reason string) {
	if err := d.getInvoker(kernel).Close(); err != nil {
		d.log.Warn("Failed to close %v after %s, failure: %v", kernel, reason, err)
	}
	kernel.Close()
}

func (d *SchedulerDaemon) cleanUp() {
	timer := time.NewTimer(cleanUpInterval)

	for {
		select {
		case <-d.closed:
			// Router is closed, clean up all kernels.
			d.kernels.Range(d.clearHandler)
			// Signal that the clean up is done.
			close(d.cleaned)
			return
		case <-timer.C:
			// Clean up expired kernels.
			d.kernels.Range(d.gcHandler)
			// Reset the timer.
			timer.Reset(cleanUpInterval)
		}
	}
}

func (d *SchedulerDaemon) clearHandler(_ string, kernel client.KernelReplicaClient) (contd bool) {
	d.getInvoker(kernel).Close()
	return true
}

func (d *SchedulerDaemon) gcHandler(kernelId string, kernel client.KernelReplicaClient) (contd bool) {
	if d.getInvoker(kernel).Expired(cleanUpInterval) {
		d.kernels.Delete(kernelId)
		if kernelId == kernel.ID() {
			d.log.Debug("Cleaned kernel %s due to expiration.", kernelId)
		} else {
			d.log.Debug("Cleaned session %s of kernel %s due to expiration.", kernelId, kernel.ID())
		}
	}
	return true
}
