package scheduling

import (
	"context"
	"fmt"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeSchedulerApi "k8s.io/kube-scheduler/extender/v1"
	"net/http"
	"time"
)

type KubernetesScheduler struct {
	*BaseScheduler

	kubeSchedulerServicePort int // Port that the Cluster Gateway's HTTP server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender.
	// TODO: There is a gap between the Host interface and the Kubernetes nodes returned by Kube API.
	kubeNodes  []v1.Node
	kubeClient KubeClient // Kubernetes client.
}

func NewKubernetesScheduler(cluster ClusterInternal, placer Placer, hostMapper HostMapper, hostSpec types.Spec,
	kubeClient KubeClient, opts *ClusterSchedulerOptions) (*KubernetesScheduler, error) {

	baseScheduler := NewBaseScheduler(cluster, placer, hostMapper, hostSpec, opts)

	kubernetesScheduler := &KubernetesScheduler{
		BaseScheduler:            baseScheduler,
		kubeClient:               kubeClient,
		kubeSchedulerServicePort: opts.SchedulerHttpPort,
	}

	baseScheduler.instance = kubernetesScheduler

	err := kubernetesScheduler.refreshClusterNodes()
	if err != nil {
		kubernetesScheduler.log.Error("Initial retrieval of Kubernetes nodes failed: %v", err)
	}

	return kubernetesScheduler, nil
}

func (s *KubernetesScheduler) ScheduleKernelReplica(spec *proto.KernelReplicaSpec, _ *Host, _ []*Host) error {
	if err := s.kubeClient.ScaleOutCloneSet(spec.Kernel.Id); err != nil {
		s.log.Error("Failed to add replica %d to kernel %s. Could not scale-up CloneSet because: %v",
			spec.ReplicaId, spec.Kernel.Id, err)
		return err
	}

	return nil
}

func (s *KubernetesScheduler) MigrateContainer(container *Container, host *Host, b bool) error {
	panic("Not implemented")
}

// DeployNewKernel is responsible for creating the necessary infrastructure ot schedule the replicas of a new
// kernel onto Host instances.
//
// In the case of KubernetesScheduler, DeployNewKernel uses the Kubernetes API to deploy the necessary Kubernetes
// Resources to create the new Kernel replicas.
func (s *KubernetesScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec, blacklistedHosts []*Host) error {
	if blacklistedHosts != nil && len(blacklistedHosts) > 0 {
		panic("Support for blacklisted hosts with Kubernetes scheduler may not have been implemented yet (I don't think it has)...")
	}

	_, err := s.kubeClient.DeployDistributedKernels(ctx, in)
	if err != nil {
		s.log.Error("Error encountered while attempting to create the Kubernetes Resources for Session %s: %v", in.Id, err)
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
