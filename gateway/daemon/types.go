package daemon

import (
	"context"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"k8s.io/client-go/kubernetes"
)

// This client is used by the Gateway to interact with Kubernetes.
type KubeClient interface {
	KubeClientset() *kubernetes.Clientset // Get the Kubernetes client.
	GatewayDaemon() *GatewayDaemon        // Get the associated Gateway daemon.
	GenerateKernelName(string) string     // Generate a name to be assigned to a Kernel.

	// Create a StatefulSet of distributed kernels for a particular Session. This should be thread-safe for unique Sessions.
	DeployDistributedKernels(context.Context, *gateway.KernelSpec) (*jupyter.ConnectionInfo, error)
	MigrateKernelReplica(context.Context, *client.DistributedKernelClient, int32, *gateway.ReplicaInfo)
}

// Represents and active, ongoing replica migration operation in which we are migrating a distributed kernel replica from one node to another.
type MigrationOperation interface {
	OperationID() string                           // Unique identifier of the migration operation.
	KernelClient() *client.DistributedKernelClient // The DistributedKernelClient of the kernel for which we're migrating a replica.
	TargetSMRNodeID() int32                        // The SMR Node ID of the replica that is being migrated.
	Completed() bool                               // Return true if the migration has been completed; otherwise, return false (i.e., if it is still ongoing).
	TargetPodName() string                         // Name of the Pod in which the target replica container is running.
}

// Component responsible for orchestrating and managing migration operations.
type MigrationManager interface {
	RegisterKernel(string)                                                                            // Inform the MigrationManager of the existence of a particular kernel so that it knows about it and can prepare to manage any future migration operations for replicas of the kernel.
	MigrateKernelReplica(context.Context, *client.DistributedKernelClient, int, *gateway.ReplicaInfo) // Initiate a migration operation for a particular Pod.
	PodCreated(interface{})                                                                           // Function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
	PodUpdated(interface{}, interface{})                                                              // Function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
	PodDeleted(interface{})                                                                           // Function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
}

// type KernelConfigMapDataSource struct {
// 	SessionId      string
// 	ConfigFileInfo *jupyter.ConfigFile
// 	ConnectionInfo *jupyter.ConnectionInfo
// }

// type SessionDef struct {
// 	SessionId           string
// 	NodeLocalMountPoint string
// 	SharedConfigDir     string
// }

// func NewSessionDef(sessionId string, nodeLocalMountPoint string, sharedConfigDir string) SessionDef {
// 	return SessionDef{
// 		SessionId:           sessionId,
// 		NodeLocalMountPoint: nodeLocalMountPoint,
// 		SharedConfigDir:     sharedConfigDir,
// 	}
// }
