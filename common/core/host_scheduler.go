package core

import (
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type HostScheduler interface {
	OnTaskStart(Kernel, *jupyter.MessageSMRLeadTask) error
}
