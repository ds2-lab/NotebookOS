package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	scheduler "k8s.io/kube-scheduler/extender/v1"
)

const (
	apiPrefix    = "/scheduler"
	filterRoute  = "/filter"
	versionRoute = "/version"
)

var (
	version string // injected via ldflags at build time
)

type schedulerExtensionImpl struct {
	// gateway domain.ClusterGateway
	log logger.Logger

	engine *gin.Engine
}

func NewSchedulerExtension( /* gateway domain.ClusterGateway */ ) SchedulerExtension {
	schedulerExtension := &schedulerExtensionImpl{
		// gateway: gateway,
		engine: gin.New(),
	}
	config.InitLogger(&schedulerExtension.log, schedulerExtension)

	schedulerExtension.setupRoutes()

	return schedulerExtension
}

func (s *schedulerExtensionImpl) setupRoutes() {
	s.log.Debug("Setting up Kubernetes Scheduler Extender routes.")

	s.engine.Use(gin.Logger())
	s.engine.Use(cors.Default())

	apiGroup := s.engine.Group(apiPrefix)
	{
		apiGroup.POST(filterRoute, s.Filter)

		apiGroup.GET(versionRoute, s.Version)
	}
}
func (s *schedulerExtensionImpl) Version(ctx *gin.Context) {
	s.log.Debug("Returning version: %s", version)
	fmt.Fprint(ctx.Writer, fmt.Sprint(version))
}

func (s *schedulerExtensionImpl) Filter(ctx *gin.Context) {
	s.log.Debug("Received FILTER request.")
	var extenderArgs scheduler.ExtenderArgs
	var extenderFilterResult *scheduler.ExtenderFilterResult

	err := ctx.BindJSON(&extenderArgs)
	if err != nil {
		s.log.Error("Failed to extract ExtenderArgs for filter call: %v", err)
		ctx.Error(err)
		extenderFilterResult = &scheduler.ExtenderFilterResult{
			Nodes:       nil,
			FailedNodes: nil,
			Error:       err.Error(),
		}
	} else {
		extenderFilterResult = &scheduler.ExtenderFilterResult{
			Nodes:       extenderArgs.Nodes,
			FailedNodes: nil,
		}

		s.log.Debug("Returning the following nodes: %v", extenderFilterResult.Nodes.Items)
	}

	ctx.JSON(http.StatusOK, extenderFilterResult)
}

func (s *schedulerExtensionImpl) Serve() {
	s.log.Debug("Scheduler Extender is starting to listen on port 80")
	if err := http.ListenAndServe(":80", s.engine); err != nil {
		s.log.Error("HTTP Server failed to listen on localhost:80 because %v", err)
		panic(err)
	}
}

func main() {
	schedulerExtension := NewSchedulerExtension()
	schedulerExtension.Serve()
}
