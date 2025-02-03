package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeSchedulerApi "k8s.io/kube-scheduler/extender/v1"
	"net/http"
	"time"
)

const (
	FilterRoute = "/filter" // Used by the Scheduler to expose an HTTP endpoint.
)

type KubernetesScheduler struct {
	*BaseScheduler

	kubeSchedulerServicePort int // Port that the Cluster Gateway's HTTP server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender.
	// TODO: There is a gap between the Host interface and the Kubernetes nodes returned by Kube API.
	kubeNodes  []v1.Node
	kubeClient scheduling.KubeClient // Kubernetes client.
}

func NewKubernetesScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	kernelProvider KernelProvider, hostSpec types.Spec, kubeClient scheduling.KubeClient, notificationBroker NotificationBroker,
	schedulingPolicy SchedulingPolicy, opts *scheduling.SchedulerOptions) (*KubernetesScheduler, error) {

	baseScheduler := newBaseSchedulerBuilder().
		WithCluster(cluster).
		WithHostMapper(hostMapper).
		WithPlacer(placer).
		WithHostSpec(hostSpec).
		WithSchedulingPolicy(schedulingPolicy).
		WithKernelProvider(kernelProvider).
		WithNotificationBroker(notificationBroker).
		WithOptions(opts).Build()

	kubernetesScheduler := &KubernetesScheduler{
		BaseScheduler:            baseScheduler,
		kubeClient:               kubeClient,
		kubeSchedulerServicePort: opts.SchedulerHttpPort,
	}

	baseScheduler.instance = kubernetesScheduler
	kubernetesScheduler.instance = kubernetesScheduler

	err := kubernetesScheduler.refreshClusterNodes()
	if err != nil {
		kubernetesScheduler.log.Error("Initial retrieval of Kubernetes nodes failed: %v", err)
	}

	return kubernetesScheduler, nil
}

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *KubernetesScheduler) HostAdded(host scheduling.Host) {
	heap.Push(s.idleHosts, host)
	s.log.Debug("Host %s (ID=%s) has been added. Cluster size: %d. Length of idle hosts: %d",
		host.GetNodeName(), host.GetID(), s.cluster.Len(), s.idleHosts.Len())
}

// HostRemoved is called by the Cluster when a Host is removed from the Cluster.
func (s *KubernetesScheduler) HostRemoved(host scheduling.Host) {
	heap.Remove(s.idleHosts, host.GetIdx(IdleHostMetadataKey))
	s.log.Debug("Host %s (ID=%s) has been removed. Cluster size: %d. Length of idle hosts: %d",
		host.GetNodeName(), host.GetID(), s.cluster.Len(), s.idleHosts.Len())
}

// findCandidateHosts is a scheduler-specific implementation for finding candidate hosts for the given kernel.
// KubernetesScheduler does not do anything special or fancy.
//
// If findCandidateHosts returns nil, rather than an empty slice, then that indicates that an error occurred.
func (s *KubernetesScheduler) findCandidateHosts(numToFind int, kernelSpec *proto.KernelSpec) ([]scheduling.Host, error) {
	// Identify the hosts onto which we will place replicas of the kernel.
	return s.placer.FindHosts([]interface{}{}, kernelSpec, numToFind, false)
}

// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
func (s *KubernetesScheduler) addReplicaSetup(kernelId string, addReplicaOp *scheduling.AddReplicaOperation) {
	s.containerEventHandler.RegisterChannel(kernelId, addReplicaOp.ReplicaStartedChannel())
}

// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
func (s *KubernetesScheduler) postScheduleKernelReplica(kernelId string, addReplicaOp *scheduling.AddReplicaOperation) {
	// In Kubernetes deployments, the key is the Pod name, which is also the kernel ID + replica suffix.
	// In Docker deployments, the container name isn't really the container's name, but its ID, which is a hash
	// or something like that.
	var (
		podOrContainerName string
		sentBeforeClosed   bool
	)

	s.log.Debug("Waiting for new replica to be created for kernel \"%s\" during AddReplicaOperation \"%s\".",
		kernelId, addReplicaOp.OperationID())

	// Always wait for the scale-out operation to complete and the new replica to be created.
	podOrContainerName, sentBeforeClosed = <-addReplicaOp.ReplicaStartedChannel()
	if !sentBeforeClosed {
		errorMessage := fmt.Sprintf("Received default value from \"Replica Started\" channel for AddReplicaOperation \"%s\": %v",
			addReplicaOp.OperationID(), addReplicaOp.String())
		s.log.Error(errorMessage)
		go s.sendErrorNotification("Channel Receive on Closed \"ReplicaStartedChannel\" Channel", errorMessage)
	} else {
		close(addReplicaOp.ReplicaStartedChannel())
	}

	s.log.Debug("New replica %d has been created for kernel %s.", addReplicaOp.ReplicaId(), kernelId)
	addReplicaOp.SetContainerName(podOrContainerName)
}

func (s *KubernetesScheduler) RemoveReplicaFromHost(_ scheduling.KernelReplica) error {
	panic("Not implemented")
}

func (s *KubernetesScheduler) ScheduleKernelReplica(spec *proto.KernelReplicaSpec, _ scheduling.Host, _ []scheduling.Host, forTraining bool) error {
	if err := s.kubeClient.ScaleOutCloneSet(spec.Kernel.Id); err != nil {
		s.log.Error("Failed to add replica %d to kernel %s. Could not scale-up CloneSet because: %v",
			spec.ReplicaId, spec.Kernel.Id, err)
		return err
	}

	return nil
}

// DeployKernelReplicas is responsible for creating the necessary infrastructure ot schedule the replicas of a new
// kernel onto Host instances.
//
// In the case of KubernetesScheduler, DeployNewKernel uses the Kubernetes API to deploy the necessary Kubernetes
// TransactionResources to create the new Kernel replicas.
func (s *KubernetesScheduler) DeployKernelReplicas(ctx context.Context, in *proto.KernelSpec, blacklistedHosts []scheduling.Host) error {
	if len(blacklistedHosts) > 0 {
		panic("Support for blacklisted hosts with Kubernetes scheduler may not have been implemented yet (I don't think it has)...")
	}

	_, err := s.kubeClient.DeployDistributedKernels(ctx, in)
	if err != nil {
		s.log.Error("Error encountered while attempting to create the Kubernetes TransactionResources for Session %s: %v", in.Id, err)
		return status.Errorf(codes.Internal, "Failed to start kernel")
	}

	return nil
}

// refreshClusterNodes updates the cached list of Kubernetes nodes.
// Returns nil on success; returns an error on failure.
func (s *KubernetesScheduler) refreshClusterNodes() error {
	nodes, err := s.kubeClient.GetKubernetesNodes()
	if err != nil {
		s.log.Error("Failed to refresh Kubernetes nodes.") // The error is printed by the KubeClient. We don't need to print it again here.
	} else {
		// TODO: There is a gap between the Host interface and the Kubernetes nodes returned by Kube API.
		s.kubeNodes = nodes
		s.lastNodeRefreshTime = time.Now()
	}

	return err // Will be nil if no error occurred.
}

// HandleKubeSchedulerFilterRequest handles a 'filter' request from the kubernetes scheduler.
func (s *KubernetesScheduler) HandleKubeSchedulerFilterRequest(ctx *gin.Context) {
	var (
		extenderArgs         kubeSchedulerApi.ExtenderArgs
		extenderFilterResult *kubeSchedulerApi.ExtenderFilterResult
		err                  error
	)

	err = ctx.BindJSON(&extenderArgs)
	if err != nil {
		s.log.Error("Received FILTER request; however, failed to extract ExtenderArgs because: %v", err)
		_ = ctx.Error(err)
		extenderFilterResult = &kubeSchedulerApi.ExtenderFilterResult{
			Nodes:       nil,
			FailedNodes: nil,
			Error:       err.Error(),
		}
	}

	s.log.Debug("Received FILTER request for Pod \"%s\" with %d node(s).", extenderArgs.Pod.Name, len(extenderArgs.Nodes.Items))

	extenderFilterResult = &kubeSchedulerApi.ExtenderFilterResult{
		Nodes: extenderArgs.Nodes,
	}

	s.log.Debug("Returning %d node(s) without any processing.", len(extenderArgs.Nodes.Items))
	ctx.JSON(http.StatusOK, extenderFilterResult)
}

func (s *KubernetesScheduler) selectViableHostForReplica(replicaSpec *proto.KernelReplicaSpec, blacklistedHosts []scheduling.Host, forTraining bool) (scheduling.Host, error) {
	panic("Not implemented")
}

// StartHttpKubernetesSchedulerService starts the HTTP service used to make scheduling decisions.
// This method should be called from its own goroutine.
func (s *KubernetesScheduler) StartHttpKubernetesSchedulerService() {
	s.log.Debug("Starting the Cluster Scheduler's HTTP Kubernetes Scheduler service.")

	app := gin.New()

	app.Use(gin.Logger())
	app.Use(cors.Default())

	app.POST(FilterRoute, s.HandleKubeSchedulerFilterRequest)

	s.log.Debug("Cluster Scheduler's HTTP Kubernetes Scheduler service is listening on port %d", s.kubeSchedulerServicePort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", s.kubeSchedulerServicePort), app); err != nil {
		s.log.Error("Cluster Scheduler's HTTP Kubernetes Scheduler service failed because: %v", err)
		panic(err)
	}
}
