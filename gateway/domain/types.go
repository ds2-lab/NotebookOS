package domain

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	FilterRoute = "/filter" // Used by the ClusterScheduler to expose an HTTP endpoint.

	// Used to monitor only for Docker container-created events corresponding to this particular project.
	// TODO: Don't hardcode this. If the "name" field in "deploy/docker/docker-compose.yml" is changed,
	// then the value of this const must be updated so that it matches the "name" field.
	DockerProjectName = "distributed_cluster"
)

type ClusterDaemonOptions struct {
	ClusterSchedulerOptions
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
	DeploymentMode                string `name:"deployment_mode" description:"Options are 'docker' and 'kubernetes'."`
}

func (o ClusterDaemonOptions) IsLocalMode() bool {
	return o.DeploymentMode == string(types.LocalMode)
}

func (o ClusterDaemonOptions) IsDockerMode() bool {
	return o.DeploymentMode == string(types.DockerMode)
}

func (o ClusterDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

type ClusterSchedulerOptions struct {
	SchedulerHttpPort             int     `name:"scheduler-http-port" description:"Port that the Cluster Gateway's kubernetes scheduler API server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender."`
	GpusPerHost                   int     `name:"gpus-per-host" description:"The number of actual GPUs that are available for use on each node/host."`
	VirtualGpusPerHost            int     `name:"num-virtual-gpus-per-node" description:"The number of virtual GPUs per host."`
	SubscribedRatioUpdateInterval float64 `name:"subscribed-ratio-update-interval" description:"The interval to update the subscribed ratio."`
	ScalingFactor                 float64 `name:"scaling-factor" description:"Defines how many hosts the cluster will provision based on busy resources"`
	ScalingInterval               int     `name:"scaling-interval" description:"Interval to call validateCapacity, 0 to disable routing scaling."`
	ScalingLimit                  float64 `name:"scaling-limit" description:"Defines how many hosts the cluster will provision at maximum based on busy resources"`
	MaximumHostsToReleaseAtOnce   int     `name:"scaling-in-limit" description:"Sort of the inverse of the ScalingLimit parameter (maybe?)"`
	ScalingOutEnaled              bool    `name:"scaling-out-enabled" description:"If enabled, the scaling manager will attempt to over-provision hosts slightly so as to leave room for fluctation. If disabled, then the Cluster will exclusivel scale-out in response to real-time demand, rather than attempt to have some hosts available in the case that demand surges."`
	ScalingBufferSize             int     `name:"scaling-buffer-size" description:"Buffer size is how many extra hosts we provision so that we can quickly scale if needed."`
	MinimumNumNodes               int     `name:"min-kubernetes-nodes" description:"The minimum number of kubernetes nodes we must have available at any time."`
}

func (o ClusterDaemonOptions) String() string {
	return fmt.Sprintf("LocalDaemonServiceName: %s, LocalDaemonServicePort: %d, SMRPort: %d, KubeNamespace: %s, UseStatefulSet: %v, HDFSNameNodeEndpoint: %s", o.LocalDaemonServiceName, o.LocalDaemonServicePort, o.SMRPort, o.KubeNamespace, o.UseStatefulSet, o.HDFSNameNodeEndpoint)
}

type ClusterGatewayOptions struct {
	config.LoggerOptions
	jupyter.ConnectionInfo
	core.CoreOptions
	ClusterDaemonOptions

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
	DebugMode       bool   `name:"debug_mode" description:"Enable the debug HTTP server."`
	DebugPort       int    `name:"debug_port" description:"The port for the debug HTTP server."`
	// DriverGRPCPort  int    `name:"driver-grpc-port" usage:"Port for the gRPC service that the workload driver connects to"`
}

type ClusterGateway interface {
	gateway.ClusterGatewayServer

	SetClusterOptions(*core.CoreOptions)
	ConnectionOptions() *jupyter.ConnectionInfo
	ClusterScheduler() ClusterScheduler                                                                   // Return the associated ClusterScheduler.
	GetClusterActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterActualGpuInfo, error) // Return the current GPU resource metrics on the node.
	KubernetesMode() bool                                                                                 // Return true if we're running in a Kubernetes cluster (rather than as a docker-compose application).
}

// Performs scheduling of kernels across the cluster.
type ClusterScheduler interface {
	// Return the associated ClusterGateway.
	ClusterGateway() ClusterGateway

	// Handle a 'filter' request from the kubernetes scheduler.
	HandleKubeSchedulerFilterRequest(ctx *gin.Context)

	// This should be called from its own goroutine.
	// Start the HTTP HTTP service used to make scheduling decisions.
	StartHttpKubernetesSchedulerService()

	// Validate the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
	// Adjust the Cluster's capacity as directed by scaling policy.
	ValidateCapacity()

	// Add a new node to the kubernetes cluster.
	// We simulate this using node taints.
	AddNode() error

	// Remove a new from the kubernetes cluster.
	// We simulate this using node taints.
	RemoveNode() error

	// Return the minimum number of nodes we must have available at any time.
	MinimumCapacity() int32

	// Try to release n idle hosts. Return the number of hosts that were actually released.
	// Error will be nil on success and non-nil if some sort of failure is encountered.
	ReleaseIdleHosts(n int32) (int, error)

	// Refresh the actual GPU usage information.
	// Returns nil on success; returns an error on failure.
	RefreshActualGpuInfo() error

	// Update the cached list of Kubernetes nodes.
	// Returns nil on success; returns an error on failure.
	RefreshKubernetesNodes() error

	// Refresh all metrics maintained/cached/required by the Cluster Scheduler,
	// including the list of current kubernetes nodes, actual and virtual GPU usage information, etc.
	//
	// Return a slice of any errors that occurred. If an error occurs while refreshing a particular piece of information,
	// then the error is recorded, and the refresh proceeds, attempting all refreshes (even if an error occurs during one refresh).
	RefreshAll() []error
}

// Watches for new Pods/Containers.
//
// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
type ContainerWatcher interface {
	// Register a channel that is used to notify waiting goroutines that the Pod/Container has started.
	//
	// Accepts as a parameter a chan string that can be used to wait until the new Container has been created.
	// The ID of the new Container will be sent over the channel when the new Container is started.
	// The error will be nil on success.
	RegisterChannel(kernelId string, startedChan chan string)
}

// This client is used by the Cluster Gateway and Cluster Scheduler to interact with Kubernetes.
type KubeClient interface {
	ContainerWatcher

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
	// Important: RegisterChannel() should be called FIRST, before this function is called.
	//
	// Parameters:
	// - kernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale-out.
	// - podStartedChannel (chan string): Used to notify waiting goroutines that the Pod has started.
	ScaleOutCloneSet(string) error

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
	KernelReplicaClient() client.DistributedKernelClient // The distributedKernelClientImpl of the kernel for which we're migrating a replica.
	KernelId() string                                    // Return the ID of the associated kernel.
	ReplicaRegistered() bool                             // Return true if the new replica has already registered with the Gateway; otherwise, return false.
	OperationID() string                                 // Unique identifier of the migration operation.
	PersistentID() string                                // Return the persistent ID of the replica.
	PodName() (string, bool)                             // Return the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	PodStarted() bool                                    // Return true if the new Pod has started.
	ReplicaPodHostname() string                          // Return the IP address of the new replica.
	ReplicaId() int32                                    // The SMR node ID to use for the new replica.
	KernelSpec() *gateway.KernelReplicaSpec              // Return the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
	SetReplicaRegistered()                               // Record that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered. This also sends a notification on the replicaRegisteredChannel.
	SetPodName(string)                                   // Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetReplicaHostname(hostname string)                  // Set the IP address of the new replica.
	SetReplicaJoinedSMR()                                // Record that the new replica has joined its SMR cluster. This also sends a notification on the ReplicaJoinedSmrChannel. NOTE: This does NOT mark the associated replica as ready. That must be done separately.
	Completed() bool                                     // Return true if the operation has completed successfully.
	ReplicaStartedChannel() chan string                  // Return the channel used to notify that the new Pod has started.
	ReplicaJoinedSmrChannel() chan struct{}              // Return the channel that is used to notify that the new replica has joined its SMR cluster.
	ReplicaRegisteredChannel() chan struct{}             // Return the channel that is used to notify that the new replica has registered with the Gateway.
	// ShouldReadDataFromHdfs() bool                        // If true, then read data from the waldir and snapdir.
	// DataDirectory() string                               // Return the path to etcd-raft data directory in HDFS.
}

// Represents and active, ongoing replica migration operation in which we are migrating a distributed kernel replica from one node to another.
type MigrationOperation interface {
	OperationID() string                                 // Unique identifier of the migration operation.
	KernelReplicaClient() client.DistributedKernelClient // The distributedKernelClientImpl of the kernel for which we're migrating a replica.
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
