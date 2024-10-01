package main

import (
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
)

type SchedulerExtension interface {
	Filter(ctx *gin.Context)
	Version(ctx *gin.Context)
	Serve()
}

type Options struct {
	config.LoggerOptions

	Port               int `name:"port" usage:"Port the HTTP service listen on." description:"Port the HTTP service listen on."`
	ClusterGatewayPort int `name:"cluster-gateway-port" description:"Port that the Cluster Gateway's HTTP kubernetes scheduler service is listening on." usage:"Port that the Cluster Gateway's HTTP kubernetes scheduler service is listening on."`
}
