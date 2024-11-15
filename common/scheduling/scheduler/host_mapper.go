package scheduler

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type HostMapper interface {
	// GetHostsOfKernel returns the Host instances on which the replicas of the specified kernel are scheduled.
	GetHostsOfKernel(kernelId string) ([]scheduling.Host, error)
}
