package proto

// Session implementation

func (x *KernelReplicaSpec) ResourceSpec() *ResourceSpec {
	return x.Kernel.ResourceSpec
}

func (x *KernelReplicaSpec) ID() string {
	return x.Kernel.Id
}
