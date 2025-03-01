package provisioner

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/types"
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

const (
	// SkipValidationKey is passed in Context of NotifyKernelRegistered to skip the connection validation step.
	SkipValidationKey contextKey = "SkipValidationKey"
)

type contextKey string

// logKernelNotFound prints an error message about not being able to find a kernel with the given ID.
//
// Specifically, if the specified ID is for a kernel that previously existed but has been stopped, then we print
// a message indicating as such. Otherwise, we print a more severe message about how the kernel simply doesn't
// exist (and never existed).
func logKernelNotFound(log logger.Logger, kernelId string, stoppedKernels hashmap.HashMap[string, time.Time]) {
	if stoppedKernels != nil {
		if stoppedAt, loaded := stoppedKernels.Load(kernelId); loaded {
			log.Warn("Could not find kernel with ID \"%s\" because that kernel was stopped %v ago at %v",
				kernelId, time.Since(stoppedAt), stoppedAt)

			return
		}
	}

	log.Error("Could not find kernel with ID \"%s\"", kernelId)
}

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
		numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
		statisticsProvider scheduling.StatisticsProvider, callbackProvider scheduling.CallbackProvider) scheduling.Kernel
}

type NetworkProvider interface {
	IP() string
	Transport() string
	ControlPort() int32
	StdinPort() int32
	HbPort() int32
	ConnectionInfo() *jupyter.ConnectionInfo
}

type KernelProvider interface {
	GetKernels() hashmap.HashMap[string, scheduling.Kernel]
	GetKernelsByKernelId() hashmap.HashMap[string, scheduling.Kernel]
}

type Provisioner struct {
	id string

	kernelProvider KernelProvider

	// kernelsStarting is a map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting hashmap.HashMap[string, chan struct{}]

	kernelShellHandler scheduling.KernelMessageHandler

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// kernelSpecs is a map from kernel ID to the *proto.KernelSpec specified when the kernel was first created.
	kernelSpecs hashmap.HashMap[string, *proto.KernelSpec]

	// waitGroups hashmap.HashMap[string, *sync.primarSemaphore]
	waitGroups hashmap.HashMap[string, *RegistrationWaitGroups]

	// kernelRegisteredNotifications is a map from notification ID to *proto.KernelRegistrationNotification
	// to keep track of the notifications that we've received so we can discard duplicates.
	kernelRegisteredNotifications hashmap.HashMap[string, *proto.KernelRegistrationNotification]

	// metricsProvider provides all metrics to the members of the scheduling package.
	metricsProvider *metrics.ClusterMetricsProvider

	// notifier is used to send notifications to the cluster dashboard.
	notifier domain.Notifier

	// networkProvider provides the Provisioner with network info required to create kernels.
	networkProvider NetworkProvider

	cluster scheduling.Cluster

	distributedClientProvider DistributedClientProvider

	kernelCallbackProvider scheduling.CallbackProvider

	opts *domain.ClusterGatewayOptions

	log logger.Logger
}

func NewProvisioner(id string, cluster scheduling.Cluster, notifier domain.Notifier, metricsProvider *metrics.ClusterMetricsProvider,
	networkProvider NetworkProvider, kernelShellHandler scheduling.KernelMessageHandler, provider KernelProvider,
	kernelCallbackProvider scheduling.CallbackProvider, opts *domain.ClusterGatewayOptions) *Provisioner {

	provisioner := &Provisioner{
		id:                            id,
		cluster:                       cluster,
		kernelProvider:                provider,
		notifier:                      notifier,
		metricsProvider:               metricsProvider,
		opts:                          opts,
		networkProvider:               networkProvider,
		kernelShellHandler:            kernelShellHandler,
		kernelCallbackProvider:        kernelCallbackProvider,
		kernelSpecs:                   hashmap.NewConcurrentMap[*proto.KernelSpec](32),
		waitGroups:                    hashmap.NewConcurrentMap[*RegistrationWaitGroups](32),
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
func (p *Provisioner) StartKernel(ctx context.Context, in *proto.KernelSpec, kernel scheduling.Kernel) (scheduling.Kernel, *proto.KernelConnectionInfo, error) {
	startTime := time.Now()

	if in == nil {
		panic("Received nil proto.KernelSpec argument to ClusterGatewayImpl::StartKernel...")
	}

	p.validateResourceSpec(in)

	p.log.Info(
		utils.LightBlueStyle.Render(
			"↪ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v]"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)

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

	if kernel == nil {
		var err error
		kernel, err = p.initNewKernel(in)
		if err != nil {
			p.log.Error("Failed to create new kernel %s because: %v", in.Id, err)
			p.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, nil, err
		}
	} else {
		p.log.Info("Restarting kernel \"%s\".", kernel.ID())
		kernel.BindSession(in.Session)

		// If we're restarting the kernel, then the resource spec being used is probably outdated.
		// So, we'll replace it with the current resource spec.
		in.ResourceSpec = proto.ResourceSpecFromSpec(kernel.ResourceSpec())
	}

	//p.kernelIdToKernel.Store(in.Id, kernel)
	//p.kernels.Store(in.Id, kernel)
	//p.kernelSpecs.Store(in.Id, in)
	//
	//// Make sure to associate the Jupyter Session with the kernel.
	//kernel.BindSession(in.Session)
	//p.kernels.Store(in.Session, kernel)

	err := p.sendStartingStatusIoPub(kernel)
	if err != nil {
		p.log.Error("Failed to send IOPub status messages during start-up of kernel %s: %v", in.Id, err)
		p.log.Error(
			utils.RedStyle.Render(
				"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
			in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
		return nil, nil, err
	}

	if p.cluster.Scheduler().Policy().ContainerLifetime() == scheduling.SingleTrainingEvent {
		p.log.Debug("Will wait to schedule container(s) for kernel %s until we receive an 'execute_request'.", in.Id)

		// Since we're not going to schedule any replicas now, we'll send an 'idle' status update in 1.5-3 seconds.
		go func() {
			time.Sleep(time.Millisecond * time.Duration(1500+rand.Intn(1500)))
			err = p.sendStartingStatusIoPub(kernel)
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
				return nil, nil, err
			}

			p.log.Error("Error while starting long-running kernel \"%s\": %v", in.Id, err)
			p.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, nil, err
		}
	}

	p.log.Debug("Created and stored new DistributedKernel %s.", in.Id)

	info := &proto.KernelConnectionInfo{
		Ip:              p.networkProvider.IP(),
		Transport:       p.networkProvider.Transport(),
		ControlPort:     p.networkProvider.ControlPort(), // int32(p.router.Socket(messaging.ControlMessage).Port),
		ShellPort:       int32(kernel.GetSocketPort(messaging.ShellMessage)),
		StdinPort:       p.networkProvider.StdinPort(), // int32(p.router.Socket(messaging.StdinMessage).Port),
		HbPort:          p.networkProvider.HbPort(),    // int32(p.router.Socket(messaging.HBMessage).Port),
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

	p.log.Info("Returning from ClusterGatewayImpl::StartKernel for kernel %s after %v:\n%v",
		kernel.ID(), time.Since(startTime), info.PrettyString())

	p.log.Info(
		utils.DarkGreenStyle.Render(
			"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Success ✓"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)

	return kernel, info, nil
}

// validateResourceSpec ensures that the given proto.KernelSpec has a valid proto.ResourceSpec.
//
// If it doesn't, then validateResourceSpec will generate one.
func (p *Provisioner) validateResourceSpec(in *proto.KernelSpec) {
	// If the resource spec of the KernelSpec argument is non-nil, then we will "sanitize" it.
	if in.ResourceSpec != nil {
		// In rare cases, the ResourceSpec will be received with certain quantities -- particularly memory -- different
		// from how they were originally sent.
		//
		// For example, there is a spec from the workload trace in which the memory is 3.908 (MB), but we receive it
		// here as "3.9079999923706055". It is still correct in the Jupyter Server and in the Gateway Provisioner (the
		// Python object), but we receive the 3.908 as 3.9079999923706055, which leads to errors.
		//
		// So, we just round everything to 3 decimal places again here, to be safe.
		originalResourceSpec := in.ResourceSpec.Clone()
		in.ResourceSpec = &proto.ResourceSpec{
			Cpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Cpu)).Round(0).InexactFloat64()),
			Memory: float32(decimal.NewFromFloat(float64(in.ResourceSpec.Memory)).Round(3).InexactFloat64()),
			Gpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Gpu)).Round(0).InexactFloat64()),
			Vram:   float32(decimal.NewFromFloat(float64(in.ResourceSpec.Vram)).Round(6).InexactFloat64()),
		}

		// For logging/debugging purposes, we check if the rounded spec and the original spec that we received are
		// unequal. If so, we'll log a message indicating as such.
		if isEqual, unequalField := in.ResourceSpec.EqualsWithField(originalResourceSpec); !isEqual {
			p.log.Warn(
				"Original ResourceSpec included in KernelSpec for new kernel \"%s\" has been rounded, and their \"%s\" fields differ.",
				in.Id, unequalField)

			p.log.Warn("Original \"%s\" field: %f. Rounded \"%s\" field: %v.",
				originalResourceSpec.GetResourceQuantity(unequalField), in.ResourceSpec.GetResourceQuantity(unequalField))
		}
	}

	p.log.Warn("KernelSpec for new kernel \"%s\" did not originally contain a ResourceSpec...")

	// Assign a default, "empty" resource spec.
	in.ResourceSpec = &proto.ResourceSpec{
		Cpu:    0,
		Memory: 0,
		Gpu:    0,
		Vram:   0,
	}
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
	msg, err := p.SendStatusMessage(kernel, "starting")
	if err != nil {
		p.log.Error("Failed to send 'starting' IOPub status message during creation of kernel \"%s\": %v",
			kernel.ID(), err)
		return err
	}

	p.log.Debug("Sent IOPub message: %v", msg)

	return nil
}

func (p *Provisioner) SendStatusMessage(kernel scheduling.Kernel, executionState string) (*messaging.JupyterMessage, error) {
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

	kernel := p.distributedClientProvider.NewDistributedKernelClient(context.Background(), in,
		p.cluster.Scheduler().Policy().NumReplicas(), p.id, p.networkProvider.ConnectionInfo(), uuid.NewString(), p.opts.DebugMode,
		p.metricsProvider, p.kernelCallbackProvider)

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

// startLongRunningKernel runs some long-running-kernel-specific start-up code.
//
// startLongRunningKernel will return as soon as the container creation process for the long-running kernel enters
// the "placement" stage, as it is very likely to succeed at that point, but the remaining process can take anywhere
// from 15 to 45 seconds (on average).
func (p *Provisioner) startLongRunningKernel(ctx context.Context, kernel scheduling.Kernel, in *proto.KernelSpec) error {
	notifyChan := make(chan interface{}, 1)
	attemptChan := make(chan scheduling.CreateReplicaContainersAttempt, 1)

	// Use a separate goroutine for this step.
	go func() {
		// We pass a new/separate context, because if we end up returning all the way back to the gRPC handler (and
		// then return from there), then the scheduling operation will fail, as the context will be cancelled (when we
		// return from the gRPC handler).
		err := p.ScheduleReplicas(context.Background(), kernel, in, attemptChan)

		if err == nil {
			notifyChan <- struct{}{}
			return
		}

		if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
			p.log.Warn("Insufficient hosts available to schedule replica container(s) of new kernel %s: %v",
				in.Id, err)
		} else {
			p.log.Error("Failed to schedule replica container(s) of new kernel %s at creation time: %v",
				in.Id, err)
		}

		// Set the kernel's status to KernelStatusError.
		kernel.InitialContainerCreationFailed()

		notifyChan <- err
	}()

	//handleSchedulingError := func(err error) error {
	//	p.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
	//
	//	// Clean up everything since we failed to create the long-running kernel.
	//	p.kernelIdToKernel.Delete(in.Id)
	//	p.kernelsStarting.Delete(in.Id)
	//	p.kernels.Delete(in.Id)
	//	p.kernelSpecs.Delete(in.Id)
	//	p.waitGroups.Delete(in.Id)
	//
	//	closeKernelError := kernel.Close()
	//	if closeKernelError != nil {
	//		p.log.Warn("Error while closing failed-to-be-created kernel \"%s\": %v", in.Id, closeKernelError)
	//	}
	//
	//	// The error should already be compatible with gRPC. But just in case it isn't...
	//	_, ok := status.FromError(err)
	//	if !ok {
	//		err = status.Error(codes.Internal, err.Error())
	//	}
	//
	//	return err
	//}

	var attempt scheduling.CreateReplicaContainersAttempt
	select {
	case <-ctx.Done(): // Original context passed to us from the gRPC handler.
		{
			err := ctx.Err()

			p.log.Error("gRPC context cancelled while scheduling replicas of new kernel \"%s\": %v",
				in.Id, err)

			if err != nil {
				return fmt.Errorf("%w: failed to schedule replicas of kernel \"%s\"", err, in.Id)
			}

			return fmt.Errorf("failed to schedule replicas of kernel \"%s\" because: %w",
				in.Id, types.ErrRequestTimedOut)
		}
	case v := <-notifyChan:
		{
			// If we received an error, then we already know that the operation failed (and we know why -- it is
			// whatever the error is/says), so we can just return the error.
			if err, ok := v.(error); ok {
				p.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
			}

			// Print a warning message because this is suspicious, but not necessarily indicative
			// that something is wrong. (It is really, really weird, though...)
			p.log.Warn("Received non-error response to creation of new, "+
				"long-running kernel \"%s\" before receiving attempt value...", in.Id)

			// If we receive a non-error response here, then we apparently already scheduled the replicas?
			// This is very unexpected, but technically it's possible...
			//
			// It's unexpected because the overhead of starting containers is high enough that the case in which
			// we receive the value from the attemptChan should occur first. We receive the attempt as soon as the
			// placement of the containers begins, which should be anywhere from 15 to 45 seconds before the
			// containers are fully created.
			return nil
		}
	case attempt = <-attemptChan:
		{
			break
		}
	}

	// Sanity check. We should only get to this point if the attempt was received from the attempt channel.
	if attempt == nil {
		panic("Expected scheduling.CreateReplicaContainersAttempt variable to be non-nil at this point.")
	}

	err := attempt.WaitForPlacementPhaseToBegin(ctx)
	if err != nil {
		p.log.Error("Error waiting for placement to begin during creation of new kernel \"%s\": %v", in.Id, err)
		return err
	}

	return attempt.Wait(ctx)

	//select {
	//// Check if there's already an error available, in which case we'll return it.
	//case v := <-notifyChan:
	//	{
	//		// If we received an error, then we already know that the operation failed (and we know why -- it is
	//		// whatever the error is/says), so we can just return the error. Otherwise, we just return optimistically.
	//		var ok bool
	//		if err, ok = v.(error); ok {
	//			p.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
	//		}
	//	}
	//default:
	//	{
	//		// No-op.
	//	}
	//}
	//
	//p.log.Debug("Placement phase began for new kernel \"%s\".", in.Id)
	//return nil
}

// ScheduleReplicas actually scheduled the replicas of the specified kernel.
//
// Important: if the attemptChan argument is non-nil, then it should be a buffered channel so that the operation
// to place the scheduling.CreateReplicaContainersAttempt into it will not block. (We don't want to get stuck
// there forever in case the caller goes away for whatever reason.)
func (p *Provisioner) ScheduleReplicas(ctx context.Context, kernel scheduling.Kernel, in *proto.KernelSpec,
	attemptChan chan<- scheduling.CreateReplicaContainersAttempt) error {

	// Check if any replicas are being migrated and, if so, then wait for them to finish being migrated.
	migrationCtx, migrateCancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer migrateCancel()

	err := kernel.WaitForMigrationsToComplete(migrationCtx)
	if err != nil {
		return err
	}

	replicasToSchedule := kernel.MissingReplicaIds()
	numReplicasToSchedule := len(replicasToSchedule)

	if numReplicasToSchedule == 0 {
		p.log.Warn("All replicas of kernel \"%s\" are already scheduled...?", kernel.ID())
		return nil
	}

	p.log.Debug("Scheduling %d replica container(s) of kernel %s.",
		numReplicasToSchedule, kernel.ID())

	var (
		startedScheduling bool
		attempt           scheduling.CreateReplicaContainersAttempt
	)

	// We'll keep executing this loop as long as the replicas of the target kernel are not scheduled.
	// We break from the loop internally if (a) we claim ownership over a container creation attempt, in which case we
	// break out so that we can orchestrate the container creation attempt, or (b) if we find that the replicas are in
	// fact scheduled. This may occur if, for example, a previous attempt concludes.
	for {
		// Try to start a new attempt at scheduling the replica container(s) of this kernel.
		startedScheduling, attempt = kernel.InitSchedulingReplicaContainersOperation()

		// If we started a new attempt, then we'll break out of the loop and orchestrate the creation of
		// the containers for the replicas of the target kernel.
		if startedScheduling {
			p.log.Debug(utils.LightBlueStyle.Render("Started attempt to schedule %d replica container(s) for kernel \"%s\"."),
				p.cluster.Scheduler().Policy().NumReplicas(), kernel.ID())
			break
		}

		// We didn't start a new scheduling attempt.
		// If the returned attempt is also nil, then that means that there was also not an active attempt.
		// So, the replicas are apparently already scheduled.
		if attempt == nil {
			p.log.Debug("Tried to start attempt to schedule %d replica container(s) for kernel \"%s\", but apparently they're already scheduled.",
				p.cluster.Scheduler().Policy().NumReplicas(), kernel.ID())

			// Double-check that the kernel's replicas are scheduled. If they are, then we'll just return entirely.
			if kernel.ReplicasAreScheduled() {
				return nil
			}

			// This would be truly bizarre, but if this occurs, then we'll just sleep briefly and then try again...
			p.log.Error("We were lead to believe that kernel %s's replicas were scheduled, but they're not...",
				kernel.ID())

			time.Sleep(time.Millisecond * (5 + time.Duration(rand.Intn(25))))
			continue
		}

		if attemptChan != nil {
			attemptChan <- attempt
		}

		// If we did not start a new attempt, then a previous attempt must still be active.
		// We'll just wait for the attempt to conclude.
		// If the scheduling is successful, then this will eventually return nil.
		// If the context passed to ScheduleReplicas has a time-out, and we time out, then this will return an error.
		p.log.Debug("Found existing 'create replica containers' operation for kernel %s that began %v ago. Waiting for operation to complete.",
			kernel.ID(), attempt.TimeElapsed())
		return attempt.Wait(ctx)
	}

	if attemptChan != nil {
		attemptChan <- attempt
	}

	// Verify that the replicas aren't scheduled.
	// If we encountered an existing scheduling operation up above that we waited for and that completed successfully,
	// then the replicas may well be available now, so we can just return.
	if kernel.ReplicasAreScheduled() {
		p.log.Debug("Replicas of kernel \"%s\" are apparently scheduled now. Returning.", kernel.ID())
		return nil
	}

	scheduleReplicasStartedEvents := make(map[int32]*metrics.ClusterEvent)
	replicaRegisteredTimestamps := make(map[int32]time.Time)
	var replicaRegisteredEventsMutex sync.Mutex

	startTime := time.Now()
	p.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		for _, replicaId := range replicasToSchedule {
			event := &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScheduleReplicasStarted,
				KernelId:            in.Id,
				ReplicaId:           replicaId,
				Timestamp:           startTime,
				TimestampUnixMillis: startTime.UnixMilli(),
			}
			statistics.ClusterEvents = append(statistics.ClusterEvents, event)
			scheduleReplicasStartedEvents[replicaId] = event
		}
	})

	// Record that this kernel is starting.
	kernelStartedChan := make(chan struct{})
	p.kernelsStarting.Store(in.Id, kernelStartedChan)
	created := NewRegistrationWaitGroups(numReplicasToSchedule)
	created.AddOnReplicaRegisteredCallback(func(replicaId int32) {
		replicaRegisteredEventsMutex.Lock()
		defer replicaRegisteredEventsMutex.Unlock()

		replicaRegisteredTimestamps[replicaId] = time.Now()
	})
	p.waitGroups.Store(in.Id, created)

	err = p.cluster.Scheduler().DeployKernelReplicas(ctx, kernel, int32(numReplicasToSchedule), []scheduling.Host{ /* No blacklisted hosts */ })
	if err != nil {
		p.log.Warn("Failed to deploy kernel replica(s) for kernel \"%s\" because: %v", kernel.ID(), err)

		// Only notify if there's an "actual" error.
		if !errors.Is(err, scheduling.ErrInsufficientHostsAvailable) && !errors.As(err, &scheduling.InsufficientResourcesError{}) {
			go p.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Create kernel \"%s\"", in.Id), err.Error())
		}

		// Record that the container creation attempt has completed with an error (i.e., it failed).
		attempt.SetDone(err)

		return err
	}

	// Wait for all replicas to be created.
	// Note that creation just means that the Container/Pod was created.
	// It does not mean that the Container/Pod has entered the active/running state.
	p.log.Debug("Waiting for replicas of new kernel %s to register. Number of kernels starting: %p.",
		in.Id, numReplicasToSchedule)
	created.Wait()
	p.log.Debug("All %d replica(s) of new kernel %s have been created and registered with their local daemons. Waiting for replicas to join their SMR cluster startTime.",
		numReplicasToSchedule, in.Id)

	// Wait until all replicas have started.
	for i := 0; i < numReplicasToSchedule; i++ {
		<-kernelStartedChan // Wait for all replicas to join their SMR cluster.
	}

	// Clean up.
	p.kernelsStarting.Delete(in.Id)
	p.log.Debug("All %d replica(s) of kernel %s have registered and joined their SMR cluster. Number of kernels starting: %p.",
		numReplicasToSchedule, in.Id, numReplicasToSchedule)

	// Sanity check.
	if kernel.Size() == 0 {
		// Record that the container creation attempt has completed with an error (i.e., it failed).
		attempt.SetDone(client.ErrFailureUnspecified)
		return status.Errorf(codes.Internal, "Failed to start kernel")
	}

	//// Map all the sessions to the kernel client.
	//for _, sess := range kernel.Sessions() {
	//	p.log.Debug("Storing kernel %v under session ID \"%s\".", kernel, sess)
	//	p.kernels.Store(sess, kernel)
	//}

	p.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		// Create corresponding events for when the replicas registered.
		for replicaId, timestamp := range replicaRegisteredTimestamps {
			timeElapsed := timestamp.Sub(startTime)
			event := &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScheduleReplicasComplete,
				KernelId:            in.Id,
				ReplicaId:           replicaId,
				Timestamp:           timestamp,
				TimestampUnixMillis: timestamp.UnixMilli(),
				Duration:            timeElapsed,
				DurationMillis:      timeElapsed.Milliseconds(),
				Metadata: map[string]interface{}{
					"start_time_unix_millis":       startTime.UnixMilli(),
					"corresponding_start_event_id": scheduleReplicasStartedEvents[replicaId].EventId,
				},
			}

			statistics.ClusterEvents = append(statistics.ClusterEvents, event)
		}
	})

	// Record that the container creation attempt has completed successfully.
	attempt.SetDone(nil)
	return nil
}

func (p *Provisioner) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	p.log.Info("Received kernel registration notification for replica %d of kernel %s from host %s (ID=%s).",
		in.ReplicaId, in.KernelId, in.NodeName, in.HostId)

	kernelId := in.KernelId

	p.log.Info("Connection info: %v", in.ConnectionInfo)
	p.log.Info("Session ID: %v", in.SessionId)
	p.log.Info("kernel ID: %v", kernelId)
	p.log.Info("Replica ID: %v", in.ReplicaId)
	p.log.Info("kernel IP: %v", in.KernelIp)
	p.log.Info("Node ID: %v", in.HostId)
	p.log.Info("Node Name: %v", in.NodeName)
	p.log.Info("Notification ID: %v", in.NotificationId)
	p.log.Info("Container name: %v", in.PodOrContainerName)

	p.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		now := time.Now()
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelReplicaRegistered,
			KernelId:            kernelId,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})
	})

	_, loaded := p.kernelRegisteredNotifications.LoadOrStore(in.NotificationId, in)
	if loaded {
		p.log.Warn("Received duplicate \"kernel Registered\" notification with ID=%s", in.NotificationId)

		go p.notifier.NotifyDashboardOfWarning("Received Duplicate \"Kernel Registered\" Notification",
			fmt.Sprintf("NotificationID=\"%s\", KernelID=\"%s\"", in.NotificationId, in.KernelId))

		return nil, status.Error(codes.InvalidArgument, types.ErrDuplicateRegistrationNotification.Error())
	}

	// p.mu.Lock()

	kernel, loaded := p.kernelProvider.GetKernels().Load(kernelId)
	if !loaded {
		p.log.Error("Could not find kernel with ID \"%s\"; however, just received 'kernel registered' notification for that kernel...", kernelId)

		title := fmt.Sprintf("Unknown Kernel \"%s\" Specified by 'Kernel Registered' Notification", kernelId)
		go p.notifier.NotifyDashboardOfError(title, "The cluster Gateway has no record of the referenced kernel.")

		// p.mu.Unlock()
		return nil, fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, kernelId)
	}

	numActiveMigrationOperations := kernel.NumActiveMigrationOperations()
	if numActiveMigrationOperations >= 1 && kernel.IsActivelyMigratingReplica(in.ReplicaId) {
		p.log.Debug("There is/are %d active add-replica operation(s) targeting kernel %s. "+
			"Assuming currently-registering replica is for an add-replica operation.",
			numActiveMigrationOperations, kernel.ID())

		// Must be holding the main mutex before calling handleMigratedReplicaRegistered.
		// It will release the lock.
		result, err := p.handleMigratedReplicaRegistered(in, kernel)

		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return result, err
	}

	return p.handleStandardKernelReplicaRegistration(ctx, kernel, in)
}

// handleStandardKernelReplicaRegistration is called to handle the registration of a scheduling.KernelReplica that is
// either being created for the very first time or as an on-demand replica to handle a single training event.
//
// The alternative to the scenarios described above is when the scheduling.KernelReplica that is registering was
// created by/during a migration operation, in which case the registration is handled by the
// handleMigratedReplicaRegistered function.
func (p *Provisioner) handleStandardKernelReplicaRegistration(ctx context.Context, kernel scheduling.Kernel,
	in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {

	// Load the 'registrationWaitGroup' struct created for this kernel's creation.
	waitGroup, loaded := p.waitGroups.Load(in.KernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing primarSemaphore associated with kernel with ID %s", in.KernelId))
	}

	connectionInfo := in.ConnectionInfo
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	replicaId := in.ReplicaId

	kernelSpec, loaded := p.kernelSpecs.Load(kernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing kernel spec for kernel with ID %s", kernelId))
	}

	host, loaded := p.cluster.GetHost(hostId)
	if !loaded {
		host, enabled, err := p.cluster.GetHostEvenIfDisabled(hostId)
		if err != nil {
			p.log.Error("Expected to find existing Host (enabled or disabled) with ID \"%v\": %v", hostId, err)
			panic(err)
		}

		if !enabled {
			p.log.Error("Registering replica %d of kernel %s on disabled host %s (ID=%s)...",
				replicaId, kernelId, host.GetNodeName(), hostId)
		} else {
			panic("what is going on")
		}

		errorTitle := fmt.Sprintf("Received Registration from Replica %d of Kernel \"%s\" On DISABLED Host %s (ID=%s)",
			replicaId, kernelId, host.GetNodeName(), hostId)
		p.notifier.NotifyDashboardOfError(errorTitle, "")

		return nil, status.Error(codes.Internal, fmt.Errorf("%w: cannot register kernel replica", scheduling.ErrHostDisabled).Error())
	}

	// If this is the first replica we're registering, then its ID should be 1.
	// The size will be 0, so we'll assign it a replica ID of 0 + 1 = 1.
	if replicaId == -1 {
		replicaId = int32(kernel.Size()) + 1
		p.log.Debug("kernel does not already have a replica ID assigned to it. Assigning ID: %p.", replicaId)
	}

	// We're registering a new replica, so the number of replicas is based on the cluster configuration.
	replicaSpec := &proto.KernelReplicaSpec{
		Kernel:      kernelSpec,
		ReplicaId:   replicaId,
		NumReplicas: int32(p.scheduler().Policy().NumReplicas()),
		WorkloadId:  kernelSpec.WorkloadId,
	}

	nodeName := in.NodeName

	if nodeName == "" || nodeName == types.DockerNode {
		if !p.opts.IsDockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		nodeName = host.GetID()
	}

	p.log.Debug("Creating new KernelReplicaClient for replica %d of kernel %s now...", in.ReplicaId, in.KernelId)

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec,
		jupyter.ConnectionInfoFromKernelConnectionInfo(connectionInfo), p.id,
		p.opts.NumResendAttempts, in.PodOrContainerName, nodeName, nil,
		nil, p.opts.MessageAcknowledgementsEnabled, kernel.PersistentID(), hostId, host, metrics.ClusterGateway,
		true, true, p.opts.DebugMode, p.metricsProvider, p.kernelReconnectionFailed,
		p.kernelRequestResubmissionFailedAfterReconnection, p.metricsProvider.UpdateClusterStatistics,
		p.opts.SubmitExecuteRequestsOneAtATime, scheduling.StandardContainer)

	session, ok := p.cluster.GetSession(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", kernelId)
		p.log.Error(errorMessage)
		p.notifier.NotifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the kernel.
	replica.SetContainer(container)

	// Register the Container with the Session.
	if err := session.AddReplica(container); err != nil {
		p.log.Error("Error while registering container %v with session %v: %v", container, session, err)
		go p.notifier.NotifyDashboardOfError("Failed to Register Container with Session", err.Error())

		// TODO: Handle this more gracefully.
		return nil, err
	}

	// p.mu.Unlock() // Need to unlock before calling ContainerStartedRunningOnHost, or deadlock can occur.

	// AddHost the Container to the Host.
	if err := host.ContainerStartedRunningOnHost(container); err != nil {
		p.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		go p.notifier.NotifyDashboardOfError("Failed to Place Container onto Host", err.Error())

		// TODO: Handle this more gracefully.
		return nil, err
	}

	p.log.Debug("Validating new kernel for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	if val := ctx.Value(SkipValidationKey); val == nil {
		err := p.validateKernelReplicaSockets(replica)
		if err != nil {
			go p.notifier.NotifyDashboardOfError(fmt.Sprintf("kernel::Validate call failed for replica %d of kernel %s",
				replica.ReplicaID(), in.KernelId), err.Error())
			return nil, err
		}
	} else {
		p.log.Warn("Skipping validation and establishment of actual network connections with newly-registered replica %d of kernel %s.",
			replica.ReplicaID(), in.KernelId)
	}

	p.log.Debug("Adding Replica for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := kernel.AddReplica(replica, host)
	if err != nil {
		p.log.Error("kernel::AddReplica call failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register(replicaId)
	p.log.Debug("SetDone registering kernel for kernel %s, replica %d on host %s. Resource spec: %v",
		kernelId, replicaId, hostId, kernelSpec.ResourceSpec)
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                              replicaId,
		Replicas:                        waitGroup.GetReplicas(),
		PersistentId:                    &persistentId,
		ResourceSpec:                    kernelSpec.ResourceSpec,
		SmrPort:                         int32(p.opts.SMRPort), // The kernel should already have this info, but we'll send it anyway.
		ShouldReadDataFromRemoteStorage: p.shouldKernelReplicaReadStateFromRemoteStorage(kernel, false),
		Ok:                              true,
	}

	p.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v",
		kernelId, replicaId, response)

	kernel.RecordContainerCreated(in.WasPrewarmContainer)

	waitGroup.Notify()
	return response, nil
}

// handleMigratedReplicaRegistered is called by NotifyKernelRegistered to handle the registration of a
// scheduling.KernelReplica that was created during a migration operation. as opposed to the scheduling.KernelReplica
//
// The alternative(s) to the scenarios described above is/are when the scheduling.KernelReplica that is registering is
// being created for the first time or as an on-demand replica to serve a single training event when using scheduling
// policies that are configured to use this approach. In either of these alternative scenarios, the registration of the
// scheduling.KernelReplica is handled by the handleStandardKernelReplicaRegistration function.
func (p *Provisioner) handleMigratedReplicaRegistered(in *proto.KernelRegistrationNotification, kernel scheduling.Kernel) (*proto.KernelRegistrationNotificationResponse, error) {
	waitGroup, loaded := p.waitGroups.Load(in.KernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing primarSemaphore associated with kernel with ID %s", in.KernelId))
	}

	// We load-and-delete the entry so that, if we migrate the same replica again in the future, then we can't load
	// the old AddReplicaOperation struct...
	key := fmt.Sprintf("%s-%d", in.KernelId, in.ReplicaId)
	addReplicaOp, ok := p.scheduler().GetAddReplicaOperationManager().LoadAndDelete(key)

	if !ok {
		errorMessage := fmt.Errorf("could not find AddReplicaOperation struct under key \"%s\"", key)
		p.log.Error(errorMessage.Error())
		p.notifier.NotifyDashboardOfError("kernel Registration Error", errorMessage.Error())

		return nil, errorMessage
	}

	if p.opts.IsDockerMode() {
		dockerContainerId := in.DockerContainerId
		if dockerContainerId == "" {
			p.log.Error("kernel registration notification did not contain docker container ID: %v", in)
			go p.notifier.NotifyDashboardOfError("Missing Docker Container ID in kernel Registration Notification",
				fmt.Sprintf("kernel registration notification for replica %d of kernel \"%s\" did not contain a valid Docker container ID",
					in.ReplicaId, in.KernelId))
		}

		addReplicaOp.SetContainerName(dockerContainerId)
	}

	host, loaded := p.cluster.GetHost(in.HostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", in.HostId)) // TODO(Ben): Handle gracefully.
	}

	// The replica spec that was specifically prepared for the new replica during the initiation of the migration operation.
	replicaSpec := addReplicaOp.KernelSpec()
	addReplicaOp.SetReplicaHostname(in.KernelIp)
	addReplicaOp.SetReplicaStarted()

	if in.NodeName == "" {
		if !p.opts.IsDockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		in.NodeName = host.GetID()
	}

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec,
		jupyter.ConnectionInfoFromKernelConnectionInfo(in.ConnectionInfo),
		p.id, p.opts.NumResendAttempts, in.PodOrContainerName, in.NodeName,
		nil, nil, p.opts.MessageAcknowledgementsEnabled, kernel.PersistentID(), in.HostId,
		host, metrics.ClusterGateway, true, true, p.opts.DebugMode, p.metricsProvider,
		p.kernelReconnectionFailed, p.kernelRequestResubmissionFailedAfterReconnection, p.metricsProvider.UpdateClusterStatistics,
		p.opts.SubmitExecuteRequestsOneAtATime, scheduling.StandardContainer)

	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	session, ok := p.cluster.GetSession(in.KernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", in.SessionId)
		p.log.Error(errorMessage)
		p.notifier.NotifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the kernel.
	replica.SetContainer(container)

	// AddHost the Container to the Host.
	p.log.Debug("Adding scheduling.Container for replica %d of kernel %s onto Host %s",
		replicaSpec.ReplicaId, addReplicaOp.KernelId(), host.GetID())
	if err = host.ContainerStartedRunningOnHost(container); err != nil {
		p.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		p.notifier.NotifyDashboardOfError("Failed to Place Container onto Host", err.Error())
		panic(err)
	}

	// p.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaID, in.kernelId, waitGroup.NumReplicas())

	// Store the new replica in the list of replicas for the kernel (at the correct position, based on the SMR node ID).
	// Then, return the list of replicas so that we can pass it to the new replica.
	// updatedReplicas := waitGroup.UpdateAndGetReplicasAfterMigration(migrationOperation.OriginalSMRNodeID()-1, in.KernelIp)
	updatedReplicas := waitGroup.AddReplica(replicaSpec.ReplicaId, in.KernelIp)

	persistentId := addReplicaOp.PersistentID()
	// dataDirectory := addReplicaOp.DataDirectory()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                              replicaSpec.ReplicaId,
		Replicas:                        updatedReplicas,
		PersistentId:                    &persistentId,
		ShouldReadDataFromRemoteStorage: p.shouldKernelReplicaReadStateFromRemoteStorage(kernel, true),
		ResourceSpec:                    replicaSpec.Kernel.ResourceSpec,
		SmrPort:                         int32(p.opts.SMRPort),
	}

	// p.mu.Unlock()

	p.log.Debug("Sending notification that replica %d of kernel \"%s\" has registered during migration \"%s\".",
		replicaSpec.ReplicaId, in.KernelId, addReplicaOp.OperationID())

	err = addReplicaOp.SetReplicaRegistered(replica)
	if err != nil {
		errorMessage := fmt.Sprintf("We're using the WRONG AddReplicaOperation... AddReplicaOperation \"%s\" has already recorded that its replica has registered: %v",
			addReplicaOp.OperationID(), addReplicaOp.String())
		p.log.Error(errorMessage)
		p.notifier.NotifyDashboardOfError("Using Incorrect AddReplicaOperation", errorMessage)
		panic(err)
	}

	//p.log.Debug("About to issue 'update replica' request for replica %d of kernel %s. Client ready: %v", replicaSpec.ReplicaId, in.KernelId, replica.IsReady())
	//
	//if p.cluster.Scheduler().Policy().NumReplicas() > 1 {
	//	p.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)
	//}

	kernel.RecordContainerCreated(in.WasPrewarmContainer)

	p.log.Debug("SetDone handling registration of added replica %d of kernel %s.", replicaSpec.ReplicaId, in.KernelId)

	return response, nil
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
func (p *Provisioner) kernelRequestResubmissionFailedAfterReconnection(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, resubmissionError error) {
	_, messageType, err := p.kernelAndTypeFromMsg(msg)
	if err != nil {
		p.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		p.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to forward \"'%s'\" request to replica %d of kernel %s following successful connection re-establishment because: %v",
		messageType, kernel.ReplicaID(), kernel.ID(), resubmissionError)
	p.log.Error(errorMessage)

	p.notifier.NotifyDashboardOfError("Connection to kernel Lost, Reconnection Succeeded, but Request Resubmission Failed", errorMessage)
}

// shouldKernelReplicaReadStateFromRemoteStorage is called by NotifyKernelRegistered and is used to determine whether
// the scheduling.KernelReplica that is registering should read/restore state from remote storage or not.
//
// The basis for this decision depends on several factors.
//
// First of all, if the scheduling policy uses short-lived containers that are created on-demand for each training
// event, then they should always restore state from intermediate remote_storage.
//
// Next, for policies that use long-lived containers (regardless of the number of replicas), the kernel should restore
// state if the kernel replicas are being recreated following an idle kernel/session reclamation.
//
// Finally, when using policy.WarmContainerPoolPolicy, scheduling.KernelReplica instances should retrieve state from
// remote storage if this is not the very first time that they're being created.
func (p *Provisioner) shouldKernelReplicaReadStateFromRemoteStorage(kernel scheduling.Kernel, forMigration bool) bool {
	policy := p.scheduler().Policy()

	// If the scheduling policy uses short-lived containers that are created on-demand for each training event,
	// then they should always restore state from intermediate remote_storage.
	if policy.ContainerLifetime() == scheduling.SingleTrainingEvent {
		return true
	}

	// If the kernel replica that is registering was created for a migration operation, then it should read and
	// restore its state from intermediate remote_storage.
	if forMigration {
		return true
	}

	// For policies that use long-lived containers (regardless of the number of replicas), the kernel should restore
	// state if the kernel replicas are being recreated following an idle kernel/session reclamation.
	if kernel.IsIdleReclaimed() {
		return true
	}

	// When using policy.WarmContainerPoolPolicy, scheduling.KernelReplica instances should retrieve state from remote
	// remote_storage if this is not the very first time that they're being created. We can test for this by checking if the
	// total number of containers created for this kernel is greater than zero.
	//
	// We also need to check if the number of containers created for this kernel is greater than or equal to the number
	// of replicas mandated by the scheduling policy. Although this isn't supported at the time of writing this, if we
	// enable warm container reuse by (e.g.,) the Static policy, then we'll be creating 3 containers when the kernel is
	// first created. We don't want the second or third replica to attempt to read state from remote storage when they
	// are being created for the first time. So, it's only once we've created at least as many containers as there are
	// replicas of an individual kernel that a new kernel replica should attempt to read state from remote storage.
	//
	// For single-replica policies, like the WarmContainerPoolPolicy, this logic will still work appropriately.
	if policy.ReuseWarmContainers() && kernel.NumContainersCreated() > 0 && kernel.NumContainersCreated() >= int32(policy.NumReplicas()) {
		return true
	}

	return false
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (p *Provisioner) kernelReconnectionFailed(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, reconnectionError error) { /* client scheduling.kernel,  */
	_, messageType, err := p.kernelAndTypeFromMsg(msg)
	if err != nil {
		p.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		p.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending \"%s\" message: %v",
		kernel.ReplicaID(), kernel.ID(), messageType, reconnectionError)
	p.log.Error(errorMessage)

	go p.notifier.NotifyDashboardOfError("Connection to kernel Lost & Reconnection Failed", errorMessage)
}

// kernelAndTypeFromMsg extracts the kernel ID and the message type from the given ZMQ message.
func (p *Provisioner) kernelAndTypeFromMsg(msg *messaging.JupyterMessage) (kernel scheduling.Kernel, messageType string, err error) {
	// This is initially the kernel's ID, which is the DestID field of the message.
	// But we may not have set a destination ID field within the message yet.
	// In this case, we'll fall back to the session ID within the message's Jupyter header.
	// This may not work either, though, if that session has not been bound to the kernel yet.
	//
	// When Jupyter clients connect for the first time, they send both a shell and a control "kernel_info_request" message.
	// This message is used to bind the session to the kernel (specifically the shell message).
	var kernelId = msg.DestinationId

	// If there is no destination ID, then we'll try to use the session ID in the message's header instead.
	if len(kernelId) == 0 {
		kernelId = msg.JupyterSession()
		p.log.Debug("Message does not have Destination ID. Using session ID \"%s\" from Jupyter header instead.", kernelId)

		// Sanity check.
		// Make sure we got a valid session ID out of the Jupyter message header.
		// If we didn't, then we'll return an error.
		if len(kernelId) == 0 {
			p.log.Error("Jupyter Session ID is invalid for message: %s", msg.String())
			err = fmt.Errorf("%w: message did not contain a destination ID, and session ID was invalid (i.e., the empty string)",
				types.ErrKernelNotFound)
			return nil, msg.JupyterMessageType(), err
		}
	}

	kernel, ok := p.kernelProvider.GetKernels().Load(kernelId) // kernelId)
	if !ok {
		logKernelNotFound(p.log, kernelId, nil) // TODO: Supply the 'stopped kernels' mapping here.
		return nil, messageType, types.ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning && p.shouldReplicasBeRunning(kernel) {
		return kernel, messageType, jupyter.ErrKernelNotReady
	}

	return kernel, messageType, nil
}

// shouldReplicasBeRunning returns a flag indicating whether the containers of kernels should be running already.
//
// For scheduling policies in which the ContainerLifetime is scheduling.LongRunning, this is true.
func (p *Provisioner) shouldReplicasBeRunning(kernel scheduling.Kernel) bool {
	// If the kernel has been idle-reclaimed, then its replicas should not be running.
	if kernel.IsIdleReclaimed() {
		return false
	}

	// If the containers are in the process of being scheduled, then it's OK that they aren't running yet.
	//
	// If the caller is handling a message that can be spoofed, then it'll be spoofed.
	//
	// If the caller is handling something like an "execute_request", then the "execute_request" handler will end
	// up waiting for the container creation operation to complete.
	if kernel.ReplicaContainersAreBeingScheduled() {
		return false
	}

	return p.scheduler().Policy().ContainerLifetime() == scheduling.LongRunning
}

func (p *Provisioner) validateKernelReplicaSockets(replica scheduling.KernelReplica) error {
	notifyChan := make(chan interface{}, 1)

	startTime := time.Now()

	go func() {
		err := replica.Validate()

		if err != nil {
			notifyChan <- err
			return
		}

		notifyChan <- struct{}{}
	}()

	notifyContext, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	select {
	case <-notifyContext.Done():
		{
			p.log.Error("Validation of sockets for new replica %d of kernel %s has timed out after %v.",
				replica.ReplicaID(), replica.KernelSpec(), time.Since(startTime))

			err := notifyContext.Err()
			if err == nil {
				err = fmt.Errorf("socket validation timed out")
			}

			return err
		}
	case v := <-notifyChan:
		{
			if err, ok := v.(error); ok {
				p.log.Error("Validation of sockets for new replica %d of kernel %s has failed after %v: %v",
					replica.ReplicaID(), replica.KernelSpec(), time.Since(startTime), err)
				return err
			}

			p.log.Debug("Successfully validated sockets of new replica %d of kernel %s in %v.",
				replica.ReplicaID(), replica.KernelSpec(), time.Since(startTime))
		}
	}

	return nil
}
