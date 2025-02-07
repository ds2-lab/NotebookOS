package scheduling

type ContainerPrewarmer interface {
	OnPrewarmedContainerUsed()
	OnKernelStopped()
	ProvisionContainer(host Host) error
	ProvisionContainers(host Host, n int) int32
	ProvisionInitialPrewarmContainers() (created int32, target int32)
}
