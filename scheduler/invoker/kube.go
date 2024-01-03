package invoker

type KubeInvoker struct {
	LocalInvoker
	dockerOpts    *jupyter.ConnectionInfo
	tempBase      string
	invokerCmd    string
	containerName string
	smrPort       int
	closing       int32
}

// InvokeWithContext starts a kernel with the given context.
func (ivk *KubeInvoker) (ivk *KubeInvoker) InvokeWithContext(context.Context, *gateway.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	return nil, nil
}

// Status returns the status of the kernel.
func (ivk *KubeInvoker) Status() (jupyter.KernelStatus, error) {
	return nil, nil 
}

// Shutdown stops the kernel gracefully.
func (ivk *KubeInvoker) Shutdown() error {
	return nil
}

// Close stops the kernel immediately.
func (ivk *KubeInvoker) Close() error {
	return nil
}

// Wait waits for the kernel to exit.
func (ivk *KubeInvoker) Wait() (jupyter.KernelStatus, error) {
	return nil, nil
}

// Expired returns true if the kernel has been stopped before the given timeout.
// If the Wait() has been called, the kernel is considered expired.
func (ivk *KubeInvoker) Expired(timeout time.Duration) bool {
	return false 
}

// OnStatusChanged registers a callback function to be called when the kernel status changes.
// The callback function is invocation sepcific and will be cleared after the kernel exits.
func (ivk *KubeInvoker) OnStatusChanged(StatucChangedHandler) {

}

// GetReplicaAddress
func (ivk *KubeInvoker) GetReplicaAddress(spec *gateway.KernelSpec, replicaId int32) string {
	return nil 
}
