package scheduler

import "github.com/zhangjyr/distributed-notebook/common/scheduling/entity"

type HostMapper interface {
	// GetHostsOfKernel returns the Host instances on which the replicas of the specified kernel are scheduled.
	GetHostsOfKernel(kernelId string) ([]*entity.Host, error)
}
