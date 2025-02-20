package main

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/gin-gonic/gin"
)

type SchedulerExtension interface {
	Filter(ctx *gin.Context)
	Version(ctx *gin.Context)
	Serve()
}

type Options struct {
	config.LoggerOptions

	Port               int `name:"port" usage:"JupyterGrpcPort the HTTP service listen on." description:"JupyterGrpcPort the HTTP service listen on."`
	ClusterGatewayPort int `name:"cluster-gateway-port" description:"JupyterGrpcPort that the Cluster Gateway's HTTP kubernetes scheduler service is listening on." usage:"JupyterGrpcPort that the Cluster Gateway's HTTP kubernetes scheduler service is listening on."`
}
