package provisioner

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"github.com/shopspring/decimal"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"math/rand"
	"sync"
	"time"
)

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
		numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
		statisticsProvider scheduling.StatisticsProvider, callbackProvider scheduling.CallbackProvider) scheduling.Kernel
}

type Provisioner struct {
	// kernelsStarting is a map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting hashmap.HashMap[string, chan struct{}]

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// kernelSpecs is a map from kernel ID to the *proto.KernelSpec specified when the kernel was first created.
	kernelSpecs hashmap.HashMap[string, *proto.KernelSpec]

	// waitGroups hashmap.HashMap[string, *sync.primarSemaphore]
	waitGroups hashmap.HashMap[string, *registrationWaitGroups]

	// kernelRegisteredNotifications is a map from notification ID to *proto.KernelRegistrationNotification
	// to keep track of the notifications that we've received so we can discard duplicates.
	kernelRegisteredNotifications hashmap.HashMap[string, *proto.KernelRegistrationNotification]

	// metricsProvider provides all metrics to the members of the scheduling package.
	metricsProvider *metrics.ClusterMetricsProvider

	// notifier is used to send notifications to the cluster dashboard.
	notifier domain.Notifier

	cluster scheduling.Cluster

	distributedClientProvider DistributedClientProvider

	opts *domain.ClusterGatewayOptions

	log logger.Logger
}

func NewProvisioner(cluster scheduling.Cluster, notifier domain.Notifier, metricsProvider *metrics.ClusterMetricsProvider,
	opts *domain.ClusterGatewayOptions) *Provisioner {

	provisioner := &Provisioner{
		cluster:                       cluster,
		notifier:                      notifier,
		metricsProvider:               metricsProvider,
		opts:                          opts,
		kernelSpecs:                   hashmap.NewConcurrentMap[*proto.KernelSpec](32),
		waitGroups:                    hashmap.NewConcurrentMap[*registrationWaitGroups](32),
		kernelRegisteredNotifications: hashmap.NewCornelkMap[string, *proto.KernelRegistrationNotification](64),
	}

	config.InitLogger(&provisioner.log, provisioner)

	return provisioner
}

func (p *Provisioner) RegisterDistributedClientProvider(distributedClientProvider DistributedClientProvider) {
	p.distributedClientProvider = distributedClientProvider
}

func (p *Provisioner) SmrReady(in *proto.SmrReadyNotification) error {
	kernelId := in.KernelId

	// First, check if this notification is from a replica of a kernel that is starting up for the very first time.
	// If so, we'll send a notification in the associated channel, and then we'll return.
	kernelStartingChan, ok := p.kernelsStarting.Load(in.KernelId)
	if ok {
		p.log.Debug("Received 'SMR-READY' notification for newly-starting replica %d of kernel %s.",
			in.ReplicaId, in.KernelId)
		kernelStartingChan <- struct{}{}
		return nil
	}

	// Check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	addReplicaOp, ok := p.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, in.ReplicaId)
	if !ok {
		p.log.Warn("Received 'SMR-READY' notification replica %d, kernel %s; however, no add-replica operation found for specified kernel replica...",
			in.ReplicaId, in.KernelId)
		return nil
	}

	if addReplicaOp.Completed() {
		log.Fatalf(utils.RedStyle.Render("Retrieved AddReplicaOperation \"%s\" targeting replica %d of kernel %s -- this operation has already completed.\n"),
			addReplicaOp.OperationID(), in.ReplicaId, kernelId)
	}

	p.log.Debug("Received SMR-READY notification for replica %d of kernel %s [AddOperation.OperationID=%v]. "+
		"Notifying awaiting goroutine now...", in.ReplicaId, kernelId, addReplicaOp.OperationID())
	addReplicaOp.SetReplicaJoinedSMR()

	return nil
}

// StartKernel launches a new kernel.
func (p *Provisioner) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	startTime := time.Now()

	if in == nil {
		panic("Received nil proto.KernelSpec argument to ClusterGatewayImpl::StartKernel...")
	}

	// If the resource spec of the KernelSpec argument is non-nil, then we will "sanitize" it.
	var originalSpec *proto.ResourceSpec
	if in.ResourceSpec != nil {
		// In rare cases, the ResourceSpec will be received with certain quantities -- particularly memory -- different
		// from how they were originally sent.
		//
		// For example, there is a spec from the workload trace in which the memory is 3.908 (MB), but we receive it
		// here as "3.9079999923706055". It is still correct in the Jupyter Server and in the Gateway Provisioner (the
		// Python object), but we receive the 3.908 as 3.9079999923706055, which leads to errors.
		//
		// So, we just round everything to 3 decimal places again here, to be safe.
		originalSpec = in.ResourceSpec.Clone()
		in.ResourceSpec = &proto.ResourceSpec{
			Cpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Cpu)).Round(0).InexactFloat64()),
			Memory: float32(decimal.NewFromFloat(float64(in.ResourceSpec.Memory)).Round(3).InexactFloat64()),
			Gpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Gpu)).Round(0).InexactFloat64()),
			Vram:   float32(decimal.NewFromFloat(float64(in.ResourceSpec.Vram)).Round(6).InexactFloat64()),
		}
	} else {
		// Assign a default, "empty" resource spec.
		in.ResourceSpec = &proto.ResourceSpec{
			Cpu:    0,
			Memory: 0,
			Gpu:    0,
			Vram:   0,
		}
	}

	p.log.Info(
		utils.LightBlueStyle.Render(
			"↪ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v]"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)

	// For logging/debugging purposes, we check if the rounded spec and the original spec that we received are
	// unequal. If so, we'll log a message indicating as such.
	if originalSpec != nil {
		// For logging/debugging purposes, we check if the rounded spec and the original spec that we received are
		// unequal. If so, we'll log a message indicating as such.
		if isEqual, unequalField := in.ResourceSpec.EqualsWithField(originalSpec); !isEqual {
			p.log.Warn(
				"Original ResourceSpec included in KernelSpec for new kernel \"%s\" has been rounded, and their \"%s\" fields differ.",
				in.Id, unequalField)

			p.log.Warn("Original \"%s\" field: %f. Rounded \"%s\" field: %v.",
				originalSpec.GetResourceQuantity(unequalField), in.ResourceSpec.GetResourceQuantity(unequalField))
		}
	} else {
		p.log.Warn("KernelSpec for new kernel \"%s\" did not originally contain a ResourceSpec...")
	}

	p.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		now := time.Now()
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelCreationStarted,
			KernelId:            in.Id,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})
	})

	var (
		kernel scheduling.Kernel
		ok     bool
		err    error
	)

	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok = p.kernels.Load(in.Id)
	if !ok {
		kernel, err = p.initNewKernel(in)
		if err != nil {
			p.log.Error("Failed to create new kernel %s because: %v", in.Id, err)
			p.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, err
		}
	} else {
		p.log.Info("Restarting kernel \"%s\".", kernel.ID())
		kernel.BindSession(in.Session)

		// If we're restarting the kernel, then the resource spec being used is probably outdated.
		// So, we'll replace it with the current resource spec.
		in.ResourceSpec = proto.ResourceSpecFromSpec(kernel.ResourceSpec())
	}

	p.kernelIdToKernel.Store(in.Id, kernel)
	p.kernels.Store(in.Id, kernel)
	p.kernelSpecs.Store(in.Id, in)

	// Make sure to associate the Jupyter Session with the kernel.
	kernel.BindSession(in.Session)
	p.kernels.Store(in.Session, kernel)

	err = p.sendStartingStatusIoPub(kernel)

	if err != nil {
		p.log.Error("Failed to send IOPub status messages during start-up of kernel %s: %v", in.Id, err)
		p.log.Error(
			utils.RedStyle.Render(
				"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
			in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
		return nil, err
	}

	if p.cluster.Scheduler().Policy().ContainerLifetime() == scheduling.SingleTrainingEvent {
		p.log.Debug("Will wait to schedule container(s) for kernel %s until we receive an 'execute_request'.", in.Id)

		// Since we're not going to schedule any replicas now, we'll send an 'idle' status update in 1.5-3 seconds.
		go func() {
			time.Sleep(time.Millisecond * time.Duration(1500+rand.Intn(1500)))
			err = p.sendIdleStatusIoPub(kernel)
			if err != nil {
				p.log.Error("Failed to send 'idle' status update for new kernel \"%s\": %v", in.Id, err)
			}
		}()

		// Since we won't be adding any replicas to the kernel right now, we need to assign a value to the
		// SignatureScheme and Key fields of the connectionInfo used by the DistributedKernelClient's server.
		//
		// If we skipped this step, then the kernel would not be able to sign messages correctly.
		kernel.SetSignatureScheme(in.SignatureScheme)
		kernel.SetKernelKey(in.Key)
	} else {
		err = p.startLongRunningKernel(ctx, kernel, in)

		if err != nil {
			if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
				p.log.Warn("Failed to start long-running kernel \"%s\" due to resource contention: %v", in.Id, err)
				p.log.Warn(
					utils.OrangeStyle.Render(
						"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
					in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
				return nil, err
			}

			p.log.Error("Error while starting long-running kernel \"%s\": %v", in.Id, err)
			p.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, err
		}
	}

	p.log.Debug("Created and stored new DistributedKernel %s.", in.Id)

	info := &proto.KernelConnectionInfo{
		Ip:              p.ip,
		Transport:       p.transport,
		ControlPort:     int32(p.router.Socket(messaging.ControlMessage).Port),
		ShellPort:       int32(kernel.GetSocketPort(messaging.ShellMessage)),
		StdinPort:       int32(p.router.Socket(messaging.StdinMessage).Port),
		HbPort:          int32(p.router.Socket(messaging.HBMessage).Port),
		IopubPort:       int32(kernel.GetSocketPort(messaging.IOMessage)),
		IosubPort:       int32(kernel.GetSocketPort(messaging.IOMessage)),
		SignatureScheme: kernel.KernelSpec().SignatureScheme,
		Key:             kernel.KernelSpec().Key,
	}

	if kernel.ReplicasAreScheduled() {
		p.log.Info("Kernel %s started after %v: %v", kernel.ID(), time.Since(startTime), info)
	} else {
		p.log.Info("Finished initialization (but not necessarily container creation) for kernel %s after %v: %v",
			kernel.ID(), time.Since(startTime), info)
	}

	session, ok := p.cluster.GetSession(kernel.ID())
	if ok {
		prom := session.SessionStarted()
		err = prom.Error()
		if err != nil {
			p.notifier.NotifyDashboardOfError(fmt.Sprintf("Error Starting Session \"%s\"", kernel.ID()), err.Error())
			panic(err)
		}
	} else {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session associated with kernel \"%s\", even though that kernel just started running successfully...", kernel.ID())
		p.log.Error(errorMessage)
		p.notifier.NotifyDashboardOfError("Session Not Found", errorMessage)
		panic(errorMessage)
	}

	p.registerKernelWithExecReqForwarder(kernel)

	p.newKernelCreated(startTime, kernel.ID())

	p.log.Info("Returning from ClusterGatewayImpl::StartKernel for kernel %s after %v:\n%v",
		kernel.ID(), time.Since(startTime), info.PrettyString())

	p.log.Info(
		utils.DarkGreenStyle.Render(
			"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Success ✓"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)

	return info, nil
}

// Return the add-replica operation associated with the given kernel ID and SMR Node ID of the new replica.
//
// This looks for the most-recently-added AddReplicaOperation associated with the specified replica of the specified kernel.
// If `mustBeActive` is true, then we skip any AddReplicaOperation structs that have already been marked as completed.
func (p *Provisioner) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (*scheduling.AddReplicaOperation, bool) {
	p.addReplicaMutex.Lock()
	defer p.addReplicaMutex.Unlock()

	p.log.Debug("Searching for an active AddReplicaOperation for replica %d of kernel \"%s\".",
		smrNodeId, kernelId)

	activeOps, ok := p.scheduler().GetActiveAddReplicaOperationsForKernel(kernelId)
	if !ok {
		return nil, false
	}

	p.log.Debug("Number of AddReplicaOperation struct(s) associated with kernel \"%s\": %d",
		kernelId, activeOps.Len())

	var op *scheduling.AddReplicaOperation
	// Iterate from newest to oldest, which entails beginning at the back.
	// We want to return the newest AddReplicaOperation that matches the replica ID for this kernel.
	for el := activeOps.Back(); el != nil; el = el.Prev() {
		p.log.Debug("AddReplicaOperation \"%s\": %s", el.Value.OperationID(), el.Value.String())
		// Check that the replica IDs match.
		// If they do match, then we either must not be bothering to check if the operation is still active, or it must still be active.
		if op == nil && el.Value.ReplicaId() == smrNodeId && el.Value.IsActive() {
			op = el.Value
		}
	}

	if op != nil {
		p.log.Debug("Returning AddReplicaOperation \"%s\": %s", op.OperationID(), op.String())
		return op, true
	}

	return nil, false
}

func (p *Provisioner) scheduler() scheduling.Scheduler {
	if p.cluster == nil {
		return nil
	}

	return p.cluster.Scheduler()
}

// sendStartingStatusIoPub is used when first starting a kernel.
//
// The Jupyter Server expects at least one IOPub message to be broadcast during the start-up procedure.
// This satisfies that requirement.
func (p *Provisioner) sendStartingStatusIoPub(kernel scheduling.Kernel) error {
	iopubSocket := kernel.Socket(messaging.IOMessage)
	if iopubSocket == nil {
		return fmt.Errorf("%w: IO socket", messaging.ErrSocketNotAvailable)
	}

	// Send the "starting" status now.
	msg, err := p.sendStatusMessage(kernel, "starting")
	if err != nil {
		p.log.Error("Failed to send 'starting' IOPub status message during creation of kernel \"%s\": %v",
			kernel.ID(), err)
		return err
	}

	p.log.Debug("Sent IOPub message: %v", msg)

	return nil
}

func (p *Provisioner) sendStatusMessage(kernel scheduling.Kernel, executionState string) (*messaging.JupyterMessage, error) {
	var (
		msg   zmq4.Msg
		err   error
		msgId = uuid.NewString()
	)
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageIdAndIdentity(msgId,
		messaging.IOStatusMessage, kernel.ID(), messaging.IOStatusMessage)

	content := map[string]string{
		"execution_state": executionState,
	}

	err = frames.EncodeContent(&content)
	if err != nil {
		p.log.Error("Failed to encode content of IOPub status message for kernel \"%s\": %v", kernel.ID(), err)
		return nil, err
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		p.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return nil, jupyter.ErrFailedToVerifyMessage
	}

	jMsg := messaging.NewJupyterMessage(&msg)

	p.log.Debug("Sending io/iopub message %s (JupyterID=\"%s\") encoding execution state/status of \"%s\" to client of kernel \"%s\" now: %v",
		jMsg.RequestId, jMsg.JupyterMessageId(), executionState, kernel.ID(), jMsg)

	err = kernel.(*client.DistributedKernelClient).SendIOMessage(jMsg)
	return jMsg, err
}

// startNewKernel is called by StartKernel when creating a brand-new kernel, rather than restarting an existing kernel.
func (p *Provisioner) initNewKernel(in *proto.KernelSpec) (scheduling.Kernel, error) {
	p.log.Debug("Did not find existing DistributedKernelClient with KernelID=\"%s\". Creating new DistributedKernelClient now.", in.Id)

	kernel := p.distributedClientProvider.NewDistributedKernelClient(context.Background(), in, p.NumReplicas(), p.id,
		p.connectionOptions, uuid.NewString(), p.DebugMode, p.MetricsProvider, d)

	p.log.Debug("Initializing Shell Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)
	_, err := kernel.InitializeShellForwarder(p.kernelShellHandler)
	if err != nil {
		if closeErr := kernel.Close(); closeErr != nil {
			p.log.Error("Error while closing kernel %s: %v.", kernel.ID(), closeErr)
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}
	p.log.Debug("Initializing IO Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)

	if _, err = kernel.InitializeIOForwarder(); err != nil {
		if closeErr := kernel.Close(); closeErr != nil {
			p.log.Error("Error while closing kernel %s: %v.", kernel.ID(), closeErr)
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Create a new Session for scheduling purposes.
	session := entity.NewSessionBuilder().
		WithContext(context.Background()).
		WithID(kernel.ID()).
		WithKernelSpec(in).
		WithTrainingTimeSampleWindowSize(p.opts.ExecutionTimeSamplingWindow).
		WithMigrationTimeSampleWindowSize(p.opts.MigrationTimeSamplingWindow).
		Build()

	p.cluster.AddSession(kernel.ID(), session)

	// Assign the Session to the DistributedKernelClient.
	kernel.SetSession(session)

	return kernel, nil
}

// newKernelCreated is to be called from StartKernel if and when the procedure succeeds.
//
// newKernelCreated pushes some metrics to Kubernetes and sends a notification to the Dashboard.
func (p *Provisioner) newKernelCreated(startTime time.Time, kernelId string) {
	// Tell the Dashboard that the kernel has successfully started running.
	go p.notifier.NotifyDashboard("kernel Started",
		fmt.Sprintf("kernel %s has started running. Launch took approximately %v from when the cluster Gateway began processing the 'create kernel' request.",
			kernelId, time.Since(startTime)), messaging.SuccessNotification)

	numActiveKernels := p.numActiveKernels.Add(1)

	p.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		statistics.NumIdleSessions += 1

		now := time.Now()
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelCreationComplete,
			KernelId:            kernelId,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})
	})

	if p.metricsProvider.PrometheusMetricsEnabled() {
		p.metricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))

		p.metricsProvider.GetGatewayPrometheusManager().TotalNumKernelsCounterVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Inc()

		p.metricsProvider.GetGatewayPrometheusManager().KernelCreationLatencyHistogram.
			Observe(float64(time.Since(startTime).Milliseconds()))
	}
}
