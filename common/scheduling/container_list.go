package scheduling

import (
	"errors"
	"strings"
)

var (
	ErrInsufficientPreemptableContainers = errors.New("insufficient preemptable containers")
)

type ContainerList []KernelContainer

func (cl ContainerList) Len() int {
	return len(cl)
}

func (cl ContainerList) Swap(i, j int) {
	cl[i], cl[j] = cl[j], cl[i]
}

func (cl ContainerList) String() string {
	if cl == nil {
		return "<nil>"
	} else if len(cl) == 0 {
		return "[]"
	}
	var msg strings.Builder
	msg.WriteString("[")
	msg.WriteString(cl[0].Host().String())
	for i := 1; i < len(cl); i++ {
		msg.WriteString(", ")
		msg.WriteString(cl[i].Host().String())
	}
	msg.WriteString("]")
	return msg.String()
}

type PenaltyContainers struct {
	ContainerList
}

func (pc *PenaltyContainers) Less(i, j int) bool {
	return pc.ContainerList[i].Session().SessionStatistics().PreemptionPriority() < pc.ContainerList[j].Session().SessionStatistics().PreemptionPriority()
}

func (pc *PenaltyContainers) Penalty(gpus float64) (float64, int, error) {
	penalty := 0.0
	preempted := 0
	for gpus > 0 {
		if preempted >= len(pc.ContainerList) {
			return penalty, preempted, ErrInsufficientPreemptableContainers
		}
		penalty += pc.ContainerList[preempted].ContainerStatistics().PreemptionPriority()
		gpus -= float64(pc.ContainerList[preempted].Session().ResourceUtilization().GetNumGpus())
		preempted++
	}
	return penalty, preempted, nil
}

func (pc *PenaltyContainers) String() string {
	if pc == nil {
		return "<nil>"
	} else if pc.Len() == 0 {
		return "[]"
	}
	var msg strings.Builder
	msg.WriteString("[")
	msg.WriteString(pc.ContainerList[0].Session().String())
	for i := 1; i < pc.Len(); i++ {
		msg.WriteString(", ")
		msg.WriteString(pc.ContainerList[i].Session().String())
	}
	msg.WriteString("]")
	return msg.String()
}
