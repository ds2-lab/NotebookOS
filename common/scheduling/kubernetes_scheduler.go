package scheduling

import (
	"fmt"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/zhangjyr/distributed-notebook/common/types"
	scheduler "k8s.io/kube-scheduler/extender/v1"
	"net/http"
	"time"
)

type KubernetesScheduler struct {
	*BaseScheduler

	kubeSchedulerServicePort int        // Port that the Cluster Gateway's HTTP server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender.
	kubeClient               KubeClient // Kubernetes client.
}

func NewKubernetesScheduler(gateway ClusterGateway, cluster Cluster, kubeClient KubeClient, opts *ClusterSchedulerOptions) (*KubernetesScheduler, error) {
	if !gateway.KubernetesMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	baseScheduler := NewClusterScheduler(gateway, cluster, opts)

	kubernetesScheduler := &KubernetesScheduler{
		BaseScheduler:            baseScheduler,
		kubeClient:               kubeClient,
		kubeSchedulerServicePort: opts.SchedulerHttpPort,
	}

	baseScheduler.instance = kubernetesScheduler

	err := kubernetesScheduler.RefreshClusterNodes()
	if err != nil {
		kubernetesScheduler.log.Error("Initial retrieval of Kubernetes nodes failed: %v", err)
	}

	return kubernetesScheduler, nil
}

// RefreshClusterNodes updates the cached list of Kubernetes nodes.
// Returns nil on success; returns an error on failure.
func (s *KubernetesScheduler) RefreshClusterNodes() error {
	if !s.gateway.KubernetesMode() {
		// Don't return an error; simply do nothing.
		return nil
	}

	nodes, err := s.kubeClient.GetKubernetesNodes()
	if err != nil {
		s.log.Error("Failed to refresh Kubernetes nodes.") // The error is printed by the KubeClient. We don't need to print it again here.
	} else {
		s.kubeNodes = nodes
		s.lastNodeRefreshTime = time.Now()
	}

	return err // Will be nil if no error occurred.
}

// HandleKubeSchedulerFilterRequest handles a 'filter' request from the kubernetes scheduler.
func (s *KubernetesScheduler) HandleKubeSchedulerFilterRequest(ctx *gin.Context) {
	var (
		extenderArgs         scheduler.ExtenderArgs
		extenderFilterResult *scheduler.ExtenderFilterResult
		err                  error
	)

	err = ctx.BindJSON(&extenderArgs)
	if err != nil {
		s.log.Error("Received FILTER request; however, failed to extract ExtenderArgs because: %v", err)
		_ = ctx.Error(err)
		extenderFilterResult = &scheduler.ExtenderFilterResult{
			Nodes:       nil,
			FailedNodes: nil,
			Error:       err.Error(),
		}
	}

	s.log.Debug("Received FILTER request for Pod \"%s\" with %d node(s).", extenderArgs.Pod.Name, len(extenderArgs.Nodes.Items))

	extenderFilterResult = &scheduler.ExtenderFilterResult{
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
