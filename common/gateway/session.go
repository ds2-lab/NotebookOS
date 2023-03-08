package gateway

import "github.com/zhangjyr/distributed-notebook/common/types"

// Session implementation
func (x *KernelReplicaSpec) Spec() types.Spec {
	return x.Kernel.Resource
}

func (x *KernelReplicaSpec) ID() string {
	return x.Kernel.Id
}
