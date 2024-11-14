package scheduler

import "github.com/zhangjyr/distributed-notebook/common/scheduling"

type Cluster interface {
	NumReplicas() int
	Len() int
	GetScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error)
	GetScaleOutCommand(targetNumNodes int32, coreLogicDoneChan chan interface{}) func()
	MetricsProvider() scheduling.MetricsProvider
}
