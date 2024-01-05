package invoker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubeInvoker struct {
	LocalInvoker
	dockerOpts     *jupyter.ConnectionInfo
	podName        string // The name of the Pod associated with the KubeInvoker.
	deploymentName string // The name of the Deployment associated with the KubeInvoker.
	smrPort        int
	closing        int32
	kubeClientset  *kubernetes.Clientset
}

func NewKubeInvoker(opts *jupyter.ConnectionInfo, deploymentName string) *KubeInvoker {
	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}
	invoker := &KubeInvoker{
		smrPort:        smrPort,
		deploymentName: deploymentName,
	}
	invoker.LocalInvoker.statusChanged = invoker.defaultStatusChangedHandler

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	invoker.kubeClientset = clientset

	return invoker
}

// InvokeWithContext starts a kernel with the given context.
func (ivk *KubeInvoker) InvokeWithContext(context.Context, *gateway.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	panic("KubeInvoker::OnStatusChanged has not yet been implemented.")

	// TODO:
	// Use KubeClient API to communicate with Kubernetes API server.
	// Create a new Pod in the associated DistributedKernel deployment.
	// Recall that each Session has its own Deployment.
}

// Status returns the status of the kernel.
func (ivk *KubeInvoker) Status() (jupyter.KernelStatus, error) {
	if ivk.status < jupyter.KernelStatusRunning {
		return 0, jupyter.ErrKernelNotLaunched
	} else {
		return ivk.status, nil
	}
}

// Shutdown stops the kernel gracefully.
func (ivk *KubeInvoker) Shutdown() error {
	panic("KubeInvoker::OnStatusChanged has not yet been implemented.")
}

// Close stops the kernel immediately.
func (ivk *KubeInvoker) Close() error {
	panic("KubeInvoker::OnStatusChanged has not yet been implemented.")
}

// Wait waits for the kernel to exit.
func (ivk *KubeInvoker) Wait() (jupyter.KernelStatus, error) {
	if ivk.podName == "" {
		return 0, jupyter.ErrKernelNotLaunched
	}

	closed := ivk.closed
	if closed != nil {
		// Wait for kernel process to exit
		<-ivk.closed
	}

	ivk.closedAt = time.Time{} // Update closedAt to extend expriation time
	return ivk.status, nil
}

// Expired returns true if the kernel has been stopped before the given timeout.
// If the Wait() has been called, the kernel is considered expired.
func (ivk *KubeInvoker) Expired(timeout time.Duration) bool {
	return false
}

// OnStatusChanged registers a callback function to be called when the kernel status changes.
// The callback function is invocation sepcific and will be cleared after the kernel exits.
func (ivk *KubeInvoker) OnStatusChanged(StatucChangedHandler) {
	panic("KubeInvoker::OnStatusChanged has not yet been implemented.")
}

// GetReplicaAddress
func (ivk *KubeInvoker) GetReplicaAddress(kernel *gateway.KernelSpec, replicaId int32) string {
	return fmt.Sprintf("%s:%d", ivk.generateKernelName(kernel, replicaId), ivk.smrPort)
}

func (ivk *KubeInvoker) generateKernelName(kernel *gateway.KernelSpec, replica_id int32) string {
	return fmt.Sprintf(DockerKernelName, fmt.Sprintf("%s-%d", kernel.Id, replica_id))
}
