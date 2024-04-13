package main

import "github.com/gin-gonic/gin"

type SchedulerExtension interface {
	Filter(ctx *gin.Context)
	Version(ctx *gin.Context)
	Serve()
}
