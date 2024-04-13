package daemon

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

type schedulerExtensionImpl struct {
	gateway ClusterGateway
	log     logger.Logger
}

func NewSchedulerExtension(gateway ClusterGateway) SchedulerExtension {
	schedulerExtension := &schedulerExtensionImpl{
		gateway: gateway,
	}
	config.InitLogger(&schedulerExtension.log, schedulerExtension)

	return schedulerExtension
}
