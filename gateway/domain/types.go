package domain

import (
	"encoding/json"
	"github.com/zhangjyr/distributed-notebook/common/proto"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

const (
	// DockerProjectName is used to monitor only for Docker container-created events corresponding to this particular project.
	// TODO: Don't hardcode this. If the "name" field in "deploy/docker/docker-compose.yml" is changed,
	// then the value of this const must be updated so that it matches the "name" field.
	DockerProjectName = "distributed_cluster"

	DockerContainerFullId  MetadataKey = "docker-container-full-id"
	DockerContainerShortId MetadataKey = "docker-container-short-id"
)

type MetadataKey string

func (k MetadataKey) String() string {
	return string(k)
}

type ClusterDaemonOptions struct {
	scheduling.ClusterSchedulerOptions `yaml:",inline"`
	LocalDaemonServiceName             string `name:"local-daemon-service-name"        json:"local-daemon-service-name"         yaml:"local-daemon-service-name"           description:"Name of the Kubernetes service that manages the local-only networking of local daemons."`
	LocalDaemonServicePort             int    `name:"local-daemon-service-port"        json:"local-daemon-service-port"         yaml:"local-daemon-service-port"           description:"Port exposed by the Kubernetes service that manages the local-only  networking of local daemons."`
	GlobalDaemonServiceName            string `name:"global-daemon-service-name"       json:"global-daemon-service-name"        yaml:"global-daemon-service-name"          description:"Name of the Kubernetes service that manages the global networking of local daemons."`
	GlobalDaemonServicePort            int    `name:"global-daemon-service-port"       json:"global-daemon-service-port"        yaml:"global-daemon-service-port"          description:"Port exposed by the Kubernetes service that manages the global networking of local daemons."`
	SMRPort                            int    `name:"smr-port"                         json:"smr-port"                          yaml:"smr-port"                            description:"Port used by the state machine replication (SMR) protocol."`
	KubeNamespace                      string `name:"kubernetes-namespace"             json:"kubernetes-namespace"              yaml:"kubernetes-namespace"                description:"Kubernetes namespace that all of these components reside in."`
	UseStatefulSet                     bool   `name:"use-stateful-set"                 json:"use-stateful-set"                  yaml:"use-stateful-set"                    description:"If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet."`
	HdfsNameNodeEndpoint               string `name:"hdfs-namenode-endpoint"           json:"hdfs-namenode-endpoint"            yaml:"hdfs-namenode-endpoint"              description:"Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this."`
	SchedulingPolicy                   string `name:"scheduling-policy"                json:"scheduling-policy"                 yaml:"scheduling-policy"                   description:"The scheduling policy to use. Options are 'default, 'static', and 'dynamic'."`
	NotebookImageName                  string `name:"notebook-image-name"              json:"notebook-image-name"               yaml:"notebook-image-name"                 description:"Name of the docker image to use for the jupyter notebook/kernel image" json:"notebook-image-name"` // Name of the docker image to use for the jupyter notebook/kernel image
	NotebookImageTag                   string `name:"notebook-image-tag"               json:"notebook-image-tag"                yaml:"notebook-image-tag"                  description:"Name of the docker image to use for the jupyter notebook/kernel image" json:"notebook-image-tag"`  // Tag to use for the jupyter notebook/kernel image
	DistributedClusterServicePort      int    `name:"distributed-cluster-service-port" json:"distributed-cluster-service-port"  yaml:"distributed-cluster-service-port"    description:"Port to use for the 'distributed cluster' service, which is used by the Dashboard."`
	DeploymentMode                     string `name:"deployment_mode"                  json:"deployment_mode"                   yaml:"deployment_mode"                     description:"Options are 'docker-compose', 'docker-swarm', and 'kubernetes'."`
	UsingWSL                           bool   `name:"using-wsl"                        json:"using-wsl"                         yaml:"using-wsl"                           description:"Flag indicating whether we're running within WSL2 (Windows Subsystem for Linux). Requires additional networking configuring for the Docker containers."`
	DockerNetworkName                  string `name:"docker_network_name"              json:"docker_network_name"               yaml:"docker_network_name"                 description:"The name of the Docker network that the container is running within. Only used in Docker mode."`
	PrometheusInterval                 int    `name:"prometheus_interval"              json:"prometheus_interval"               yaml:"prometheus_interval"                 description:"Frequency in seconds of how often to publish metrics to Prometheus. So, setting this to 5 means we publish metrics roughly every 5 seconds."`
	PrometheusPort                     int    `name:"prometheus_port"                  json:"prometheus_port"                   yaml:"prometheus_port"                     description:"The port on which this local daemon will serve Prometheus metrics. Default/suggested: 8089."`
}

// IsLocalMode returns true if the deployment mode is specified as "local".
func (o ClusterDaemonOptions) IsLocalMode() bool {
	return o.DeploymentMode == string(types.LocalMode)
}

// IsDockerMode returns true if the deployment mode is specified as either "docker-swarm" or "docker-compose".
func (o ClusterDaemonOptions) IsDockerMode() bool {
	return o.IsDockerComposeMode() || o.IsDockerSwarmMode()
}

// IsDockerSwarmMode returns true if the deployment mode is specified as "docker-swarm".
func (o ClusterDaemonOptions) IsDockerSwarmMode() bool {
	return o.DeploymentMode == string(types.DockerSwarmMode)
}

// IsDockerComposeMode returns true if the deployment mode is specified as "docker-compose".
func (o ClusterDaemonOptions) IsDockerComposeMode() bool {
	return o.DeploymentMode == string(types.DockerComposeMode)
}

// IsKubernetesMode returns true if the deployment mode is specified as "kubernetes".
func (o ClusterDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

func (o ClusterDaemonOptions) String() string {
	//return fmt.Sprintf("LocalDaemonServiceName: %s, LocalDaemonServicePort: %d, SMRPort: %d, KubeNamespace: %s, UseStatefulSet: %v, HdfsNameNodeEndpoint: %s", o.LocalDaemonServiceName, o.LocalDaemonServicePort, o.SMRPort, o.KubeNamespace, o.UseStatefulSet, o.HdfsNameNodeEndpoint)
	out, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}

	return string(out)
}

type ClusterGatewayOptions struct {
	config.LoggerOptions   `yaml:",inline"`
	jupyter.ConnectionInfo `yaml:",inline"`
	ClusterDaemonOptions   `yaml:",inline"`

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
	DebugMode       bool   `name:"debug_mode" description:"Enable the debug HTTP server."`
	DebugPort       int    `name:"debug_port" description:"The port for the debug HTTP server."`
	// DriverGRPCPort  int    `name:"driver-grpc-port" usage:"Port for the gRPC service that the workload driver connects to"`
}

type AddReplicaOperation interface {
	KernelReplicaClient() *client.DistributedKernelClient // KernelReplicaClient returns the client.DistributedKernelClient of the kernel for which we're migrating a replica.
	KernelId() string                                     // KernelId returns the ID of the associated kernel.
	ReplicaRegistered() bool                              // ReplicaRegistered returns true if the new replica has already registered with the Gateway; otherwise, return false.
	OperationID() string                                  // OperationID returns the unique identifier of the migration operation.
	PersistentID() string                                 // PersistentID returns the persistent ID of the replica.
	PodName() (string, bool)                              // PodName returns the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	PodStarted() bool                                     // PodStarted returns true if the new Pod has started.
	ReplicaPodHostname() string                           // ReplicaPodHostname returns the IP address of the new replica.
	ReplicaId() int32                                     // ReplicaId returns the SMR node ID to use for the new replica.
	KernelSpec() *proto.KernelReplicaSpec                 // KernelSpec returns the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
	SetReplicaRegistered()                                // SetReplicaRegistered records that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered. This also sends a notification on the replicaRegisteredChannel.
	SetPodName(string)                                    // SetPodName sets the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetReplicaHostname(hostname string)                   // SetReplicaHostname sets the IP address of the new replica.
	SetReplicaJoinedSMR()                                 // SetReplicaJoinedSMR records that the new replica has joined its SMR cluster. This also sends a notification on the ReplicaJoinedSmrChannel. NOTE: This does NOT mark the associated replica as ready. That must be done separately.
	Completed() bool                                      // Completed return true if the operation has completed successfully. This is the inverse of `AddReplicaOperation::IsActive`.
	IsActive() bool                                       // IsActive returns true if the operation has not yet finished. This is the inverse of `AddReplicaOperation::Completed`.
	ReplicaStartedChannel() chan string                   // ReplicaStartedChannel returns the channel used to notify that the new Pod has started.
	ReplicaJoinedSmrChannel() chan struct{}               // ReplicaJoinedSmrChannel returns the channel that is used to notify that the new replica has joined its SMR cluster.
	ReplicaRegisteredChannel() chan struct{}              // ReplicaRegisteredChannel returns the channel that is used to notify that the new replica has registered with the Gateway.
	GetMetadata(MetadataKey) (interface{}, bool)          // GetMetadata returns a piece of metadata associated with the given MetadataKey and a bool indicating whether the metadata was successfully retrieved.
	SetMetadata(MetadataKey, interface{})                 // SetMetadata stores a piece of metadata under the given MetadataKey.

	// ShouldReadDataFromHdfs() bool                        // If true, then read data from the waldir and snapdir.
	// DataDirectory() string                               // Return the path to etcd-raft data directory in HDFS.
}

// MigrationOperation represents an active, ongoing replica migration operation in which we are migrating a distributed kernel replica from one node to another.
type MigrationOperation interface {
	OperationID() string                                  // Unique identifier of the migration operation.
	KernelReplicaClient() *client.DistributedKernelClient // The distributedKernelClientImpl of the kernel for which we're migrating a replica.
	KernelId() string                                     // Return the ID of the associated kernel.
	OriginalSMRNodeID() int32                             // The (original) SMR Node ID of the replica that is being migrated. The new replica will have a different ID.
	PersistentID() string                                 // Get the persistent ID of the replica we're migrating.
	NewReplicaJoinedSMR() bool                            // Returns true if the new replica itself has joined the SMR cluster. Otherwise, returns false.
	SetNewReplicaJoinedSMR()                              // Record that the new replica has joined its SMR cluster.
	OldPodStopped() bool                                  // Returns true if the original Pod of the replica has stopped. Otherwise, returns false.
	NewPodStarted() bool                                  // Return true if the new Pod has started.
	Completed() bool                                      // Returns true if the migration has been completed; otherwise, returns false (i.e., if it is still ongoing).
	GetNewReplicaRegistered() bool                        // Return true if the new replica has already registered with the Gateway; otherwise, return false.
	OldPodName() string                                   // Name of the Pod in which the target replica container is running.
	NewPodName() (string, bool)                           // Return the name of the newly-created Pod that will host the migrated replica. Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
	SetNewPodName(string)                                 // Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
	SetOldPodStopped()                                    // Record that the old Pod (containing the replica to be migrated) has stopped.
	Wait()                                                // Block and wait until the migration operation has completed.
	NotifyNewReplicaRegistered()                          // Record that the new replica for this migration operation has registered with the Gateway. Will panic if we've already recorded that the new replica has registered.
	Broadcast()                                           // Broadcast (Notify) any go routines waiting for the migration operation to complete. Should only be called once the migration operation has completed.
	GetNewReplicaKernelSpec() *proto.KernelReplicaSpec    // Return the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
	NewReplicaHostname() string                           // Return the IP address of the new replica.
	SetNewReplicaHostname(hostname string)                // Set the IP address of the new replica.
}

// MigrationManager is a component responsible for orchestrating and managing migration operations.
type MigrationManager interface {
	// RegisterKernel informs the MigrationManager of the existence of a particular kernel so that it knows about it and can prepare to manage any future migration operations for replicas of the kernel.
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

// AddReplicaWaitOptions define options for add replica operations.
// We always wait for the scale-out to occur.
type AddReplicaWaitOptions interface {
	WaitRegistered() bool  // If true, wait for the replica registration to occur.
	WaitSmrJoined() bool   // If true, wait for the SMR joined notification.
	ReuseSameNodeId() bool // If true, reuse the same SMR node ID for the new node.
}
