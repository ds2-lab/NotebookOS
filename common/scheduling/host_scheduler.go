package scheduling

import (
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type LocalDaemonClient interface {
	OnTaskStart(Kernel, *jupyter.MessageSMRLeadTask) error
	// OnTaskEnd(Kernel, *jupyter.Message)
}
