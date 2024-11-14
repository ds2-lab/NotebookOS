package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// KubeClient is used by the Cluster Gateway and Cluster Scheduler to interact with Kubernetes.
type KubeClient interface {
	ContainerWatcher

	Clientset() *kubernetes.Clientset // Get the Kubernetes client.

	// DeployDistributedKernels creates a StatefulSet of distributed kernels for a particular Session. This should be thread-safe for unique Sessions.
	DeployDistributedKernels(context.Context, *proto.KernelSpec) (*types.ConnectionInfo, error)

	// DeleteCloneset deletes the CloneSet for the kernel identified by the given ID.
	DeleteCloneset(kernelId string) error

	// GetKubernetesNodes returns a list of the current kubernetes nodes.
	GetKubernetesNodes() ([]corev1.Node, error)

	// GetKubernetesNode returns the node with the given name, or nil of that node cannot be found.
	GetKubernetesNode(string) (*corev1.Node, error)

	// Add the specified label to the specified node.
	// Returns nil on success; otherwise, returns an error.
	// AddLabelToNode(nodeId string, labelKey string, labelValue string) error

	// Remove the specified label from the specified node.
	// Returns nil on success; otherwise, returns an error.
	// RemoveLabelFromNode(nodeId string, labelKey string, labelValue string) error

	// ScaleOutCloneSet scales up a CloneSet by increasing its number of replicas by 1.
	// Important: RegisterChannel() should be called FIRST, before this function is called.
	//
	// Parameters:
	// - KernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale-out.
	// - podStartedChannel (chan string): Used to notify waiting goroutines that the Pod has started.
	ScaleOutCloneSet(string) error

	// ScaleInCloneSet scales down a CloneSet by decreasing its number of replicas by 1.
	// Returns a chan string that can be used to wait until the new Pod has been created.
	// The name of the new Pod will be sent over the channel when the new Pod is started.
	// The error will be nil on success.
	//
	// Parameters:
	// - KernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale in
	// - oldPodName (string): The name of the Pod that we'd like to delete during the scale-in operation.
	// - podStoppedChannel (chan struct{}): Used to notify waiting goroutines that the Pod has stopped.
	ScaleInCloneSet(string, string, chan struct{}) error
}
