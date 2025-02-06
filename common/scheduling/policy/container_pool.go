package policy

// WarmContainerPoolPolicy defines the scheduling policy of the "middle ground" approach, in which training requests
// are served exclusively by a pool of warm scheduling.KernelContainer instances.
//
// Of course, if there are no warm scheduling.KernelContainer instances available when an "execute_request" arrives,
// then a new scheduling.KernelContainer instance will be created on demand to serve the request.
//
// WarmContainerPoolPolicy is similar to FcfsBatchSchedulingPolicy with the main difference being that the
// ReuseWarmContainers property of WarmContainerPoolPolicy is true, whereas WarmContainerPoolPolicy is false for
// FcfsBatchSchedulingPolicy. As a result, scheduling.KernelContainer instances are returned to the warm pool after
// use when the WarmContainerPoolPolicy is active.
//
// In real life, this would present security/privacy concerns; however, WarmContainerPoolPolicy is not meant to be a
// usable, real-world policy. It is simply meant to reflect a hypothetical policy or situation in which there are some
// warm containers available. A real-world instantiation of WarmContainerPoolPolicy would potentially rely on some
// other research work to handle the actual provisioning and maintenance of a useful warm scheduling.KernelContainer
// pool. Such a pool would have a good balance of scheduling.KernelContainer instances with different runtime
// dependencies and software environments to satisfy the varied requirements of different users.
type WarmContainerPoolPolicy struct {
}

// ReuseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
// placed back into the warm KernelContainer pool, or if it should simply be terminated.
//
// ReuseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
//
// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
func (p *WarmContainerPoolPolicy) ReuseWarmContainers() bool {
	return true
}
