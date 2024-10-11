package daemon

import (
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"

	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
)

type AddReplicaOperation struct {
	id                    string                               // Unique identifier of the add operation.
	kernelId              string                               // ID of the kernel for which a replica is being added.
	client                *client.DistributedKernelClient      // distributedKernelClientImpl of the kernel for which we're migrating a replica.
	smrNodeId             int32                                // The SMR Node ID of the replica that is being added.
	podOrContainerStarted bool                                 // True if a new Pod has been started for the replica that is being added. Otherwise, false.
	replicaJoinedSMR      bool                                 // True if the new replica has joined the SMR cluster. Otherwise, false.
	podOrContainerName    string                               // Name of the new Pod that was started to host the added replica.
	replicaRegistered     bool                                 // If true, then new replica has registered with the Gateway.
	persistentId          string                               // Persistent ID of replica.
	replicaHostname       string                               // The IP address of the new replica.
	spec                  *proto.KernelReplicaSpec             // Spec for the new replica that is created during the add operation.
	dataDirectory         string                               // Path to etcd-raft data directory in HDFS.
	metadata              hashmap.HashMap[string, interface{}] // Arbitrary metadata associated with this domain.AddReplicaOperation.

	podOrContainerStartedChannel chan string   // Used to notify that the new Pod has started.
	replicaRegisteredChannel     chan struct{} // Used to notify that the new replica has registered with the Gateway.
	replicaJoinedSmrChannel      chan struct{} // Used to notify that the new replica has joined its SMR cluster.
}

func NewAddReplicaOperation(client *client.DistributedKernelClient, spec *proto.KernelReplicaSpec, dataDirectory string) *AddReplicaOperation {
	op := &AddReplicaOperation{
		id:                           uuid.New().String(),
		client:                       client,
		spec:                         spec,
		smrNodeId:                    spec.ReplicaId,
		kernelId:                     spec.Kernel.Id,
		persistentId:                 *spec.PersistentId,
		podOrContainerStarted:        false,
		replicaJoinedSMR:             false,
		replicaRegistered:            false,
		dataDirectory:                dataDirectory,
		metadata:                     hashmap.NewCornelkMap[string, interface{}](4),
		podOrContainerStartedChannel: make(chan string),
		replicaRegisteredChannel:     make(chan struct{}),
		replicaJoinedSmrChannel:      make(chan struct{}),
	}

	return op
}

// True if the new replica should read data from HDFS; otherwise, false.
// I guess, for addReplicaOps, this will always be true?
// So, maybe this field is unnecessary...
// func (op *AddReplicaOperation) ShouldReadDataFromHdfs() bool {
// 	return op.shouldReadDataFromHdfs
// }

// DataDirectory returns the path to etcd-raft data directory in HDFS.
func (op *AddReplicaOperation) DataDirectory() string {
	return op.dataDirectory
}

// ToString
func (op *AddReplicaOperation) String() string {
	return fmt.Sprintf("AddReplicaOperation[ID=%s,KernelID=%s,ReplicaID=%d,Completed=%v,NewPodName=%s,"+
		"PersistentID=%s,NewPodOrContainerStarted=%v,NewPodOrContainerName=%s,NewReplicaRegistered=%v,NewReplicaJoinedSmr=%v]",
		op.id, op.kernelId, op.smrNodeId, op.Completed(), op.podOrContainerName, op.persistentId,
		op.podOrContainerStarted, op.podOrContainerName, op.replicaRegistered, op.replicaJoinedSMR)
}

// ReplicaStartedChannel returns the channel that is used to notify that the new Pod or Container has started.
func (op *AddReplicaOperation) ReplicaStartedChannel() chan string {
	return op.podOrContainerStartedChannel
}

// ReplicaRegisteredChannel returns the channel that is used to notify that the new replica has registered with the Gateway.
func (op *AddReplicaOperation) ReplicaRegisteredChannel() <-chan struct{} {
	return op.replicaRegisteredChannel
}

// ReplicaJoinedSmrChannel returns the channel that is used to notify that the new replica has joined its SMR cluster.
func (op *AddReplicaOperation) ReplicaJoinedSmrChannel() chan struct{} {
	return op.replicaJoinedSmrChannel
}

// Completed returns true if the operation has completed successfully, which requires the following three criteria to be true:
// - The new Pod has started.
// - The new replica has registered with its local daemon and the Gateway.
// - The new replica has joined its SMR cluster.
//
// This is the inverse of `AddReplicaOperation::Active`.
func (op *AddReplicaOperation) Completed() bool {
	return op.podOrContainerStarted && op.replicaRegistered && op.replicaJoinedSMR
}

// IsActive returns true if the operation has not yet finished. This is the inverse of `AddReplicaOperation::Completed`.
func (op *AddReplicaOperation) IsActive() bool {
	return !op.Completed()
}

// OperationID returns the unique identifier of the add operation.
func (op *AddReplicaOperation) OperationID() string {
	return op.id
}

// KernelReplicaClient returns the *client.DistributedKernelClient of the kernel for which we're migrating a replica.
func (op *AddReplicaOperation) KernelReplicaClient() *client.DistributedKernelClient {
	return op.client
}

// KernelId returns the kernel ID of the kernel whose replica we're adding
func (op *AddReplicaOperation) KernelId() string {
	return op.kernelId
}

// ReplicaId returns the SMR node ID to use for the new replica.
func (op *AddReplicaOperation) ReplicaId() int32 {
	return op.smrNodeId
}

// SetContainerName sets the name of the newly-created Pod or Container that will host the added replica.
// This also records that this operation's new pod has started.
func (op *AddReplicaOperation) SetContainerName(name string) {
	if op.podOrContainerStarted {
		panic(fmt.Sprintf("Migration operation %s already has a new pod/container (with name/id = \"%s\").", op.id, op.podOrContainerName))
	}

	op.podOrContainerStarted = true
	op.podOrContainerName = name
}

// PersistentID Returns the persistent ID of the replica.
func (op *AddReplicaOperation) PersistentID() string {
	return op.persistentId
}

// PodOrContainerName Returns the name of the newly-created Pod that will host the added replica.
// Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
func (op *AddReplicaOperation) PodOrContainerName() (string, bool) {
	if op.podOrContainerStarted {
		return op.podOrContainerName, true
	} else {
		return "", false
	}
}

// ReplicaJoinedSMR Returns true if a new Pod has been started for the replica that is being added. Otherwise, returns false.
func (op *AddReplicaOperation) ReplicaJoinedSMR() bool {
	return op.replicaJoinedSMR
}

// SetReplicaJoinedSMR Records that the new replica has joined its SMR cluster.
// This also sends a notification on the ReplicaJoinedSmrChannel and marks the associated replica as ready.
func (op *AddReplicaOperation) SetReplicaJoinedSMR() {
	op.replicaJoinedSMR = true
	op.replicaJoinedSmrChannel <- struct{}{}

	// COMMENTED OUT:
	// We do this step elsewhere in the Cluster Gateway. If we ever use SmrNodeAdded again, then maybe we'd
	// want to uncomment this? Because SmrNodeAdded calls SetReplicaJoinedSMR, and there was a bug in which
	// SmrReady did NOT call SetReplicaJoinedSMR and instead just explicitly got a reference to the
	// replicaJoinedSmrChannel and put a struct in it directly, so SetReplicaJoinedSMR was never called,
	// and thus the commented-out code below was not being executed.
	//
	// The bug was that, by not calling SetReplicaJoinedSMR, we weren't setting replicaJoinedSMR to true, which
	// we needed to do so that the AddReplicaOperation was considered to have completed.
	//
	// Mark the new replica as being ready.
	//replica, err := op.client.GetReplicaByID(op.smrNodeId)
	//if err != nil {
	//	panic(fmt.Sprintf("Cannot find new replica with ID %d for kernel %s.", op.smrNodeId, op.kernelId))
	//}
	//
	//replica.SetReady()
}

// PodStarted Returns true if the new Pod has started.
func (op *AddReplicaOperation) PodStarted() bool {
	return op.podOrContainerStarted
}

// ReplicaRegistered Returns true if the new replica has already registered with the Gateway; otherwise, return false.
func (op *AddReplicaOperation) ReplicaRegistered() bool {
	return op.replicaRegistered
}

// SetReplicaRegistered Records that the new replica for this migration operation has registered with the Gateway.
// Will panic if we've already recorded that the new replica has registered.
// This also sends a notification on the replicaRegisteredChannel.
func (op *AddReplicaOperation) SetReplicaRegistered() {
	op.replicaRegistered = true
	op.replicaRegisteredChannel <- struct{}{} // KernelID isn't needed.
}

// KernelSpec Returns the *gateway.KernelReplicaSpec for the new replica that is created during the add operation.
func (op *AddReplicaOperation) KernelSpec() *proto.KernelReplicaSpec {
	return op.spec
}

// ReplicaPodHostname Returns the IP address of the new replica.
func (op *AddReplicaOperation) ReplicaPodHostname() string {
	return op.replicaHostname
}

// SetReplicaHostname Sets the IP address of the new replica.
func (op *AddReplicaOperation) SetReplicaHostname(hostname string) {
	op.replicaHostname = hostname
}

// GetMetadata returns a piece of metadata associated with the given MetadataKey, or nil if no such metadata exists.
func (op *AddReplicaOperation) GetMetadata(key domain.MetadataKey) (value interface{}, loaded bool) {
	value, loaded = op.metadata.Load(key.String())
	return
}

// SetMetadata stores a piece of metadata under the given MetadataKey.
func (op *AddReplicaOperation) SetMetadata(key domain.MetadataKey, value interface{}) {
	op.metadata.Store(key.String(), value)
}

type addReplicaWaitOptionsImpl struct {
	waitRegistered  bool
	waitSmrJoined   bool
	reuseSameNodeId bool
}

func NewAddReplicaWaitOptions(waitRegistered bool, waitSmrJoined bool, reuseSameNodeId bool) domain.AddReplicaWaitOptions {
	return &addReplicaWaitOptionsImpl{
		waitRegistered:  waitRegistered,
		waitSmrJoined:   waitSmrJoined,
		reuseSameNodeId: reuseSameNodeId,
	}
}

// ReuseSameNodeId does the following: If true, reuse the same SMR node ID for the new node.
func (o *addReplicaWaitOptionsImpl) ReuseSameNodeId() bool {
	return o.reuseSameNodeId
}

// WaitRegistered does the following: If true, wait for the replica registration to occur.
func (o *addReplicaWaitOptionsImpl) WaitRegistered() bool {
	return o.waitRegistered
}

// WaitSmrJoined does the following: If true, wait for the SMR joined notification.
func (o *addReplicaWaitOptionsImpl) WaitSmrJoined() bool {
	return o.waitSmrJoined
}
