package daemon

import (
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"k8s.io/client-go/kubernetes"
)

// This client is used by the Gateway to interact with Kubernetes.
type KubeClient interface {
	KubeClientset() *kubernetes.Clientset // Get the Kubernetes client.
	GatewayDaemon() *GatewayDaemon        // Get the associated Gateway daemon.
	GenerateKernelName(string) string     // Generate a name to be assigned to a Kernel.

	// Create a StatefulSet of distributed kernels for a particular Session. This should be thread-safe for unique Sessions.
	CreateKernelStatefulSet(*gateway.KernelSpec) (*jupyter.ConnectionInfo, error)
}

type SessionDef struct {
	SessionId           string
	NodeLocalMountPoint string
	SharedConfigDir     string
}

func NewSessionDef(sessionId string, nodeLocalMountPoint string, sharedConfigDir string) SessionDef {
	return SessionDef{
		SessionId:           sessionId,
		NodeLocalMountPoint: nodeLocalMountPoint,
		SharedConfigDir:     sharedConfigDir,
	}
}
