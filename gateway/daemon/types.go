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

	// Initiate a migration operation for a particular replica of a particular kernel. The migration will be carried out automatically by the migration manager once it has been initiated.
	InitiateKernelMigration(context.Context, *client.DistributedKernelClient, int32, string) error

	// Return the migration operation associated with the given Pod name, such that the Pod with the given name was created for the given migration operation.
	GetMigrationOperationByNewPod(string) (MigrationOperation, bool)

	// Check if the given Migration Operation has finished. This is called twice: when the new replica registers with the Gateway,
	// and when the old Pod is deleted. Whichever of those two events happens last will be the one that designates the operation has having completed.
	CheckIfMigrationCompleted(MigrationOperation) bool

	// Wait for us to receive a pod-created notification for the given Pod, which managed to start running
	// and register with us before we received the pod-created notification. Once received, return the
	// associated migration operation.
	WaitForNewPodNotification(string) MigrationOperation

	// Return the migration operation associated with the given Kernel ID and SMR Node ID.
	// Currently unused (at the time of writing this comment).
	// GetMigrationOperationByKernelIdAndReplicaId(string, int) (MigrationOperation, bool)
}

// Represents and active, ongoing replica migration operation in which we are migrating a distributed kernel replica from one node to another.
type MigrationOperation interface {
	OperationID() string                           // Unique identifier of the migration operation.
	KernelClient() *client.DistributedKernelClient // The DistributedKernelClient of the kernel for which we're migrating a replica.
	KernelId() string                              // Return the ID of the associated kernel.
	TargetSMRNodeID() int32                        // The SMR Node ID of the replica that is being migrated.
	PersistentID() string                          // Get the persistent ID of the replica we're migrating.
	NewPodStarted() bool                           // Returns true if a new Pod has been started for the replica that is being migrated. Otherwise, returns false.
	OldPodStopped() bool                           // Returns true if the original Pod of the replica has stopped. Otherwise, returns false.
	Completed() bool                               // Returns true if the migration has been completed; otherwise, returns false (i.e., if it is still ongoing).
	OldPodName() string                            // Name of the Pod in which the target replica container is running.
	NewPodName() (string, bool)                    // Return the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	SetNewPodName(string)                          // Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetOldPodStopped()                             // Record that the old Pod (containing the replica to be migrated) has stopped.
	Wait()                                         // Block and wait until the migration operation has completed.
	GetNewReplicaRegistered() bool                 // Return true if the new replica has already registered with the Gateway; otherwise, return false.
	NotifyNewReplicaRegistered()                   // Record that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered.
	Broadcast()                                    // Broadcast (Notify) any go routines waiting for the migration operation to complete. Should only be called once the migration operation has completed.
}

// Component responsible for orchestrating and managing migration operations.
type MigrationManager interface {
	// Inform the MigrationManager of the existence of a particular kernel so that it knows about it and can prepare to manage any future migration operations for replicas of the kernel.
	RegisterKernel(string)

	// Initiate a migration operation for a particular Pod. The migration will be carried out automatically by the migration manager once it has been initiated.
	InitiateKernelMigration(context.Context, *client.DistributedKernelClient, int32, string) error

	// Return the migration operation associated with the given Pod name, such that the Pod with the given name was created for the given migration operation.
	GetMigrationOperationByNewPod(string) (MigrationOperation, bool)

	// Return the migration operation associated with the given Kernel ID and SMR Node ID.
	GetMigrationOperationByKernelIdAndReplicaId(string, int) (MigrationOperation, bool)

	// Wait for us to receive a pod-created notification for the given Pod, which managed to start running
	// and register with us before we received the pod-created notification. Once received, return the
	// associated migration operation.
	WaitForNewPodNotification(string) MigrationOperation

	// Check if the given Migration Operation has finished. This is called twice: when the new replica registers with the Gateway,
	// and when the old Pod is deleted. Whichever of those two events happens last will be the one that designates the operation has having completed.
	CheckIfMigrationCompleted(MigrationOperation) bool

	PodCreated(interface{})              // Function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
	PodUpdated(interface{}, interface{}) // Function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
	PodDeleted(interface{})              // Function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
}

// Used to patch the metadata of a Pod.
type LabelPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
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
