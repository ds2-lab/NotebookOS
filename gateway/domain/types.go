package domain

import (
	"context"
	"fmt"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type ClusterDaemonOptions struct {
	LocalDaemonServiceName        string `name:"local-daemon-service-name" description:"Name of the Kubernetes service that manages the local-only networking of local daemons."`
	LocalDaemonServicePort        int    `name:"local-daemon-service-port" description:"Port exposed by the Kubernetes service that manages the local-only  networking of local daemons."`
	GlobalDaemonServiceName       string `name:"global-daemon-service-name" description:"Name of the Kubernetes service that manages the global networking of local daemons."`
	GlobalDaemonServicePort       int    `name:"global-daemon-service-port" description:"Port exposed by the Kubernetes service that manages the global networking of local daemons."`
	SMRPort                       int    `name:"smr-port" description:"Port used by the state machine replication (SMR) protocol."`
	KubeNamespace                 string `name:"kube-namespace" description:"Kubernetes namespace that all of these components reside in."`
	UseStatefulSet                bool   `name:"use-stateful-set" description:"If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet."`
	HDFSNameNodeEndpoint          string `name:"hdfs-namenode-endpoint" description:"Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this."`
	SchedulingPolicy              string `name:"scheduling-policy" description:"The scheduling policy to use. Options are 'default, 'static', and 'dynamic'."`
	NotebookImageName             string `name:"notebook-image-name" description:"Name of the docker image to use for the jupyter notebook/kernel image" json:"notebook-image-name"` // Name of the docker image to use for the jupyter notebook/kernel image
	NotebookImageTag              string `name:"notebook-image-tag" description:"Name of the docker image to use for the jupyter notebook/kernel image" json:"notebook-image-tag"`   // Tag to use for the jupyter notebook/kernel image
	DistributedClusterServicePort int    `name:"distributed-cluster-service-port" description:"Port to use for the 'distributed cluster' service, which is used by the Dashboard."`
	SchedulerHttpPort             int    `name:"scheduler-http-port" description:"Port that the Cluster Gateway's kubernetes scheduler API server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender."`
}

func (o ClusterDaemonOptions) String() string {
	return fmt.Sprintf("LocalDaemonServiceName: %s, LocalDaemonServicePort: %d, SMRPort: %d, KubeNamespace: %s, UseStatefulSet: %v, HDFSNameNodeEndpoint: %s", o.LocalDaemonServiceName, o.LocalDaemonServicePort, o.SMRPort, o.KubeNamespace, o.UseStatefulSet, o.HDFSNameNodeEndpoint)
}

type Options struct {
	config.LoggerOptions
	jupyter.ConnectionInfo
	core.CoreOptions
	ClusterDaemonOptions

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
	// DriverGRPCPort  int    `name:"driver-grpc-port" usage:"Port for the gRPC service that the workload driver connects to"`
}

type ClusterGateway interface {
	gateway.ClusterGatewayServer

	SetClusterOptions(*core.CoreOptions)
	ConnectionOptions() *jupyter.ConnectionInfo
}

// This client is used by the Gateway to interact with Kubernetes.
type KubeClient interface {
	KubeClientset() *kubernetes.Clientset // Get the Kubernetes client.
	ClusterGateway() ClusterGateway       // Get the associated Gateway daemon.

	// Create a StatefulSet of distributed kernels for a particular Session. This should be thread-safe for unique Sessions.
	DeployDistributedKernels(context.Context, *gateway.KernelSpec) (*jupyter.ConnectionInfo, error)

	// Delete the Cloneset for the kernel identified by the given ID.
	DeleteCloneset(kernelId string) error

	// Return a list of the current kubernetes nodes.
	GetKubernetesNodes() ([]corev1.Node, error)

	// Return the node with the given name, or nil of that node cannot be found.
	GetKubernetesNode(string) (*corev1.Node, error)

	// Add the specified label to the specified node.
	// Returns nil on success; otherwise, returns an error.
	// AddLabelToNode(nodeId string, labelKey string, labelValue string) error

	// Remove the specified label from the specified node.
	// Returns nil on success; otherwise, returns an error.
	// RemoveLabelFromNode(nodeId string, labelKey string, labelValue string) error

	// Scale-up a CloneSet by increasing its number of replicas by 1.
	// Accepts as a parameter a chan string that can be used to wait until the new Pod has been created.
	// The name of the new Pod will be sent over the channel when the new Pod is started.
	// The error will be nil on success.
	//
	// Parameters:
	// - kernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale-out.
	// - podStartedChannel (chan string): Used to notify waiting goroutines that the Pod has started.
	ScaleOutCloneSet(string, chan string) error

	// Scale-down a CloneSet by decreasing its number of replicas by 1.
	// Returns a chan string that can be used to wait until the new Pod has been created.
	// The name of the new Pod will be sent over the channel when the new Pod is started.
	// The error will be nil on success.
	//
	// Parameters:
	// - kernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale in
	// - oldPodName (string): The name of the Pod that we'd like to delete during the scale-in operation.
	// - podStoppedChannel (chan struct{}): Used to notify waiting goroutines that the Pod has stopped.
	ScaleInCloneSet(string, string, chan struct{}) error
}

type AddReplicaOperation interface {
	KernelClient() client.DistributedKernelClient // The distributedKernelClientImpl of the kernel for which we're migrating a replica.
	KernelId() string                             // Return the ID of the associated kernel.
	ReplicaRegistered() bool                      // Return true if the new replica has already registered with the Gateway; otherwise, return false.
	OperationID() string                          // Unique identifier of the migration operation.
	PersistentID() string                         // Return the persistent ID of the replica.
	PodName() (string, bool)                      // Return the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	PodStarted() bool                             // Return true if the new Pod has started.
	ReplicaPodHostname() string                   // Return the IP address of the new replica.
	ReplicaId() int32                             // The SMR node ID to use for the new replica.
	KernelSpec() *gateway.KernelReplicaSpec       // Return the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
	SetReplicaRegistered()                        // Record that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered. This also sends a notification on the replicaRegisteredChannel.
	SetPodName(string)                            // Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetReplicaHostname(hostname string)           // Set the IP address of the new replica.
	SetReplicaJoinedSMR()                         // Record that the new replica has joined its SMR cluster. This also sends a notification on the ReplicaJoinedSmrChannel. NOTE: This does NOT mark the associated replica as ready. That must be done separately.
	Completed() bool                              // Return true if the operation has completed successfully.
	PodStartedChannel() chan string               // Return the channel used to notify that the new Pod has started.
	ReplicaJoinedSmrChannel() chan struct{}       // Return the channel that is used to notify that the new replica has joined its SMR cluster.
	ReplicaRegisteredChannel() chan struct{}      // Return the channel that is used to notify that the new replica has registered with the Gateway.
	DataDirectory() string                        // Return the path to etcd-raft data directory in HDFS.
}

// Represents and active, ongoing replica migration operation in which we are migrating a distributed kernel replica from one node to another.
type MigrationOperation interface {
	OperationID() string                                 // Unique identifier of the migration operation.
	KernelClient() client.DistributedKernelClient        // The distributedKernelClientImpl of the kernel for which we're migrating a replica.
	KernelId() string                                    // Return the ID of the associated kernel.
	OriginalSMRNodeID() int32                            // The (original) SMR Node ID of the replica that is being migrated. The new replica will have a different ID.
	PersistentID() string                                // Get the persistent ID of the replica we're migrating.
	NewReplicaJoinedSMR() bool                           // Returns true if the new replica itself has joined the SMR cluster. Otherwise, returns false.
	SetNewReplicaJoinedSMR()                             // Record that the new replica has joined its SMR cluster.
	OldPodStopped() bool                                 // Returns true if the original Pod of the replica has stopped. Otherwise, returns false.
	NewPodStarted() bool                                 // Return true if the new Pod has started.
	Completed() bool                                     // Returns true if the migration has been completed; otherwise, returns false (i.e., if it is still ongoing).
	GetNewReplicaRegistered() bool                       // Return true if the new replica has already registered with the Gateway; otherwise, return false.
	OldPodName() string                                  // Name of the Pod in which the target replica container is running.
	NewPodName() (string, bool)                          // Return the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	SetNewPodName(string)                                // Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetOldPodStopped()                                   // Record that the old Pod (containing the replica to be migrated) has stopped.
	Wait()                                               // Block and wait until the migration operation has completed.
	NotifyNewReplicaRegistered()                         // Record that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered.
	Broadcast()                                          // Broadcast (Notify) any go routines waiting for the migration operation to complete. Should only be called once the migration operation has completed.
	GetNewReplicaKernelSpec() *gateway.KernelReplicaSpec // Return the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
	NewReplicaHostname() string                          // Return the IP address of the new replica.
	SetNewReplicaHostname(hostname string)               // Set the IP address of the new replica.
}

// Component responsible for orchestrating and managing migration operations.
type MigrationManager interface {
	// Inform the MigrationManager of the existence of a particular kernel so that it knows about it and can prepare to manage any future migration operations for replicas of the kernel.
	RegisterKernel(string)

	// Initiate a migration operation for a particular Pod. The migration will be carried out automatically by the migration manager once it has been initiated.
	// InitiateKernelMigration(context.Context, *client.distributedKernelClientImpl, int32, *gateway.KernelReplicaSpec) (string, error)

	// Return the migration operation associated with the given Pod name, such that the Pod with the given name was created for the given migration operation.
	// GetMigrationOperationByNewPod(string) (MigrationOperation, bool)

	// Return the migration operation associated with the given Kernel ID and SMR Node ID.
	// GetMigrationOperationByKernelIdAndNewReplicaId(string, int32) (MigrationOperation, bool)

	// Wait for us to receive a pod-created notification for the given Pod, which managed to start running
	// and register with us before we received the pod-created notification. Once received, return the
	// associated migration operation.
	// WaitForNewPodNotification(string) AddReplicaOperation

	// Check if the given Migration Operation has finished. This is called twice: when the new replica registers with the Gateway,
	// and when the old Pod is deleted. Whichever of those two events happens last will be the one that designates the operation has having completed.
	// CheckIfMigrationCompleted(MigrationOperation) bool

	PodCreated(interface{})              // Function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
	PodUpdated(interface{}, interface{}) // Function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
	PodDeleted(interface{})              // Function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
}

// We always wait for the scale-out to occur.
type AddReplicaWaitOptions interface {
	WaitRegistered() bool  // If true, wait for the replica registration to occur.
	WaitSmrJoined() bool   // If true, wait for the SMR joined notification.
	ReuseSameNodeId() bool // If true, reuse the same SMR node ID for the new node.
}
