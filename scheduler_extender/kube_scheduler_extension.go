package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	scheduler "k8s.io/kube-scheduler/extender/v1"
)

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

	engine *gin.Engine
	port   int
}

func NewSchedulerExtension(opts *Options /* gateway domain.ClusterGateway */) SchedulerExtension {
	schedulerExtension := &schedulerExtensionImpl{
		// gateway: gateway,
		engine: gin.New(),
		port:   opts.Port,
	}
	config.InitLogger(&schedulerExtension.log, schedulerExtension)

	schedulerExtension.setupRoutes()

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
	s.log.Debug("Scheduler Extender v%s is starting to listen on port %d", version, s.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", s.port), s.engine); err != nil {
		s.log.Error("HTTP Server failed to listen on localhost:80 because %v", err)
		panic(err)
	}
}

func main() {
	flags, err := config.ValidateOptions(&options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}

	schedulerExtension := NewSchedulerExtension(&options)
	schedulerExtension.Serve()
}
