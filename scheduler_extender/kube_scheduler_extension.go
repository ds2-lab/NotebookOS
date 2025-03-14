package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	scheduler "k8s.io/kube-scheduler/extender/v1"
)

// TODO(Ben):
// We can avoid granting admin permissions to the entire Gateway Pod via:
// - https://github.com/kubernetes/kubernetes/issues/66020#issuecomment-880150743
// - https://github.com/googleforgames/agones/blob/e048859c6ff3da0d4b299f915113993aa49c7865/pkg/gameservers/controller.go#L555
// - https://github.com/googleforgames/agones/blob/e048859c6ff3da0d4b299f915113993aa49c7865/pkg/apis/agones/v1/gameserver.go#L682

const (
	// apiPrefix    = "/scheduler"
	filterRoute  = "/filter"
	versionRoute = "/version"
)

var (
	options = Options{}
	version string // injected via ldflags at build time
)

type schedulerExtensionImpl struct {
	// gateway domain.ClusterGateway
	log logger.Logger

	engine             *gin.Engine
	port               int    // JupyterGrpcPort that we should listen on.
	clusterGatewayAddr string // Address of the HTTP kubernetes scheduler service exposed by the Cluster Gateway.

	filterAddr string // Endpoint to forward/proxy filter requests to.
}

func NewSchedulerExtension(opts *Options /* gateway domain.ClusterGateway */) SchedulerExtension {
	schedulerExtension := &schedulerExtensionImpl{
		// gateway: gateway,
		engine:             gin.New(),
		port:               opts.Port,
		clusterGatewayAddr: fmt.Sprintf("http://127.0.0.1:%d", opts.ClusterGatewayPort),
	}
	config.InitLogger(&schedulerExtension.log, schedulerExtension)

	schedulerExtension.setupRoutes()
	schedulerExtension.filterAddr = fmt.Sprintf("%s/filter", schedulerExtension.clusterGatewayAddr)

	return schedulerExtension
}

func (s *schedulerExtensionImpl) setupRoutes() {
	s.log.Debug("Setting up Kubernetes Scheduler Extender routes.")

	s.engine.Use(gin.Logger())
	s.engine.Use(cors.Default())

	s.engine.POST(filterRoute, s.Filter)
	s.engine.GET(versionRoute, s.Version)
}

func (s *schedulerExtensionImpl) Version(ctx *gin.Context) {
	fmt.Fprint(ctx.Writer, fmt.Sprint(version))
}

// Forward a request to the Cluster Gateway for processing.
func (s *schedulerExtensionImpl) proxyFilterRequest(req *http.Request) (*http.Response, error) {
	proxyReq, err := http.NewRequest(req.Method, s.filterAddr, req.Body)
	if err != nil {
		s.log.Error("Failed to create proxy request because: %v", err)
		return nil, err
	}

	proxyReq.Header.Set("Host", req.Host)
	proxyReq.Header.Set("X-Forwarded-For", req.RemoteAddr)

	for header, values := range req.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	client := &http.Client{}
	proxyRes, err := client.Do(proxyReq)
	if err != nil {
		s.log.Error("Error while proxying request to Cluster Gateway: %v", err)
	}

	return proxyRes, err
}

func (s *schedulerExtensionImpl) Filter(ctx *gin.Context) {
	s.log.Debug("Received FILTER request.")

	resp, err := s.proxyFilterRequest(ctx.Request)
	if err != nil {
		ctx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.Error("Failed to read response of proxied request because: %v", err)
		ctx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	var extenderFilterResult scheduler.ExtenderFilterResult
	err = json.Unmarshal(data, &extenderFilterResult)

	if err != nil {
		s.log.Error("Failed to unmarshal response of proxied request because: %v", err)
		ctx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	nodeNames := make([]string, 0, len(extenderFilterResult.Nodes.Items))
	for _, node := range extenderFilterResult.Nodes.Items {
		nodeNames = append(nodeNames, node.Name)
	}

	s.log.Debug("Returning the following nodes (%d): %v", len(nodeNames), nodeNames)

	ctx.JSON(http.StatusOK, extenderFilterResult)
}

func (s *schedulerExtensionImpl) Serve() {
	s.log.Debug("Scheduler Extender v%s is listening on port %d", version, s.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", s.port), s.engine); err != nil {
		s.log.Error("HTTP Server failed to listen on localhost:%d because %v", s.port, err)
		panic(err)
	}
}

func main() {
	flags, err := config.ValidateOptions(&options)
	if errors.Is(err, config.ErrPrintUsage) {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}

	schedulerExtension := NewSchedulerExtension(&options)
	schedulerExtension.Serve()
}
