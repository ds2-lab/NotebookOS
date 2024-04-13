package daemon

import (
	v1 "k8s.io/api/core/v1"
	scheduler "k8s.io/kubernetes/pkg/scheduler/apis/extender/v1"
)

type Predicate struct {
	Name string
	Func func(pod v1.Pod, node v1.Node) (bool, error)
}

func (p Predicate) Handler(args scheduler.ExtenderArgs) *scheduler.ExtenderFilterResult {
	pod := args.Pod
	canSchedule := make([]v1.Node, 0, len(args.Nodes.Items))
	canNotSchedule := make(map[string]string)

	for _, node := range args.Nodes.Items {
		result, err := p.Func(*pod, node)
		if err != nil {
			canNotSchedule[node.Name] = err.Error()
		} else {
			if result {
				canSchedule = append(canSchedule, node)
			}
		}
	}

	result := scheduler.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: canSchedule,
		},
		FailedNodes: canNotSchedule,
		Error:       "",
	}

	return &result
}
