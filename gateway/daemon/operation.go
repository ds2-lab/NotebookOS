package daemon

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
)

type addReplicaOperationImpl struct {
	id                string                         // Unique identifier of the add operation.
	kernelId          string                         // ID of the kernel for which a replica is being added.
	client            client.DistributedKernelClient // distributedKernelClientImpl of the kernel for which we're migrating a replica.
	smrNodeId         int32                          // The SMR Node ID of the replica that is being added.
	podStarted        bool                           // True if a new Pod has been started for the replica that is being added. Otherwise, false.
	replicaJoinedSMR  bool                           // True if the new replica has joined the SMR cluster. Otherwise, false.
	podName           string                         // Name of the new Pod that was started to host the added replica.
	replicaRegistered bool                           // If true, then new replica has registered with the Gateway.
	persistentId      string                         // Persistent ID of replica.
	replicaHostname   string                         // The IP address of the new replica.
	spec              *gateway.KernelReplicaSpec     // Spec for the new replica that is created during the add operation.
	dataDirectory     string                         // Path to etcd-raft data directory in HDFS.

	podStartedChannel        chan string   // Used to notify that the new Pod has started.
	replicaRegisteredChannel chan struct{} // Used to notify that the new replica has registered with the Gateway.
	replicaJoinedSmrChannel  chan struct{} // Used to notify that the new replica has joined its SMR cluster.
}

func NewAddReplicaOperation(client client.DistributedKernelClient, spec *gateway.KernelReplicaSpec, dataDirectory string) domain.AddReplicaOperation {
	op := &addReplicaOperationImpl{
		id:                       uuid.New().String(),
		client:                   client,
		spec:                     spec,
		smrNodeId:                spec.ReplicaId,
		kernelId:                 spec.Kernel.Id,
		persistentId:             *spec.PersistentId,
		podStarted:               false,
		replicaJoinedSMR:         false,
		replicaRegistered:        false,
		dataDirectory:            dataDirectory,
		podStartedChannel:        make(chan string),   // TODO: These were originally buffered. Is it OK if they're not buffered?
		replicaRegisteredChannel: make(chan struct{}), // TODO: These were originally buffered. Is it OK if they're not buffered?
		replicaJoinedSmrChannel:  make(chan struct{}), // TODO: These were originally buffered. Is it OK if they're not buffered?
	}

	return op
}

// True if the new replica should read data from HDFS; otherwise, false.
// I guess, for addReplicaOps, this will always be true?
// So, maybe this field is unnecessary...
// func (op *addReplicaOperationImpl) ShouldReadDataFromHdfs() bool {
// 	return op.shouldReadDataFromHdfs
// }

// Return the path to etcd-raft data directory in HDFS.
func (op *addReplicaOperationImpl) DataDirectory() string {
	return op.dataDirectory
}

// ToString
func (op *addReplicaOperationImpl) String() string {
	return fmt.Sprintf("AddReplicaOperation[ID=%s,KernelID=%s,ReplicaID=%d,Completed=%v,NewPodName=%s,PersistentID=%s,NewReplicaRegistered=%v]", op.id, op.kernelId, op.smrNodeId, op.Completed(), op.podName, op.persistentId, op.replicaRegistered)
}

// Return the channel that is used to notify that the new Pod has started.
func (op *addReplicaOperationImpl) ReplicaStartedChannel() chan string {
	return op.podStartedChannel
}

// Return the channel that is used to notify that the new replica has registered with the Gateway.
func (op *addReplicaOperationImpl) ReplicaRegisteredChannel() chan struct{} {
	return op.replicaRegisteredChannel
}

// Return the channel that is used to notify that the new replica has joined its SMR cluster.
func (op *addReplicaOperationImpl) ReplicaJoinedSmrChannel() chan struct{} {
	return op.replicaJoinedSmrChannel
}

// Returns true if the operation has completed successfully, which requires the following three criteria to be true:
// - The new Pod has started.
// - The new replica has registered with its local daemon and the Gateway.
// - The new replica has joined its SMR cluster.
//
// This is the inverse of `AddReplicaOperation::Active`.
func (op *addReplicaOperationImpl) Completed() bool {
	return op.podStarted && op.replicaRegistered && op.replicaJoinedSMR
}

// Returns true if the operation has not yet finished. This is the inverse of `AddReplicaOperation::Completed`.
func (op *addReplicaOperationImpl) IsActive() bool {
	return !op.Completed()
}

// Unique identifier of the add operation.
func (op *addReplicaOperationImpl) OperationID() string {
	return op.id
}

// distributedKernelClientImpl of the kernel for which we're migrating a replica.
func (op *addReplicaOperationImpl) KernelReplicaClient() client.DistributedKernelClient {
	return op.client
}

// Notify any go routines waiting for the add operation to complete. Should only be called once the add operation has completed.
func (op *addReplicaOperationImpl) KernelId() string {
	return op.kernelId
}

// The SMR node ID to use for the new replica.
func (op *addReplicaOperationImpl) ReplicaId() int32 {
	return op.smrNodeId
}

// Set the name of the newly-created Pod that will host the added replica. This also records that this operation's new pod has started.
func (op *addReplicaOperationImpl) SetPodName(newPodName string) {
	if op.podStarted {
		panic(fmt.Sprintf("Migration operation %s already has a new pod (pod %s).", op.id, op.podName))
	}

	op.podStarted = true
	op.podName = newPodName
}

// Return the persistent ID of the replica.
func (op *addReplicaOperationImpl) PersistentID() string {
	return op.persistentId
}

// Return the name of the newly-created Pod that will host the added replica.
// Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
func (op *addReplicaOperationImpl) PodName() (string, bool) {
	if op.podStarted {
		return op.podName, true
	} else {
		return "", false
	}
}

// Returns true if a new Pod has been started for the replica that is being added. Otherwise, returns false.
func (op *addReplicaOperationImpl) ReplicaJoinedSMR() bool {
	return op.replicaJoinedSMR
}

// Record that the new replica has joined its SMR cluster.
// This also sends a notification on the ReplicaJoinedSmrChannel and marks the associated replica as ready.
func (op *addReplicaOperationImpl) SetReplicaJoinedSMR() {
	op.replicaJoinedSMR = true
	op.replicaJoinedSmrChannel <- struct{}{}

	// Mark the new replica as being ready.
	replica, err := op.client.GetReplicaByID(op.smrNodeId)
	if err != nil {
		panic(fmt.Sprintf("Cannot find new replica with ID %d for kernel %s.", op.smrNodeId, op.kernelId))
	}

	replica.SetReady()
}

// Return true if the new Pod has started.
func (op *addReplicaOperationImpl) PodStarted() bool {
	return op.podStarted
}

// Return true if the new replica has already registered with the Gateway; otherwise, return false.
func (op *addReplicaOperationImpl) ReplicaRegistered() bool {
	return op.replicaRegistered
}

// Record that the new replica for this migration operation has registered with the Gateway.
// Will panic if we've already recorded that the new replica has registered.
// This also sends a notification on the replicaRegisteredChannel.
func (op *addReplicaOperationImpl) SetReplicaRegistered() {
	op.replicaRegistered = true
	op.replicaRegisteredChannel <- struct{}{} // KernelID isn't needed.
}

// Return the *gateway.KernelReplicaSpec for the new replica that is created during the add operation.
func (op *addReplicaOperationImpl) KernelSpec() *gateway.KernelReplicaSpec {
	return op.spec
}

// Return the IP address of the new replica.
func (op *addReplicaOperationImpl) ReplicaPodHostname() string {
	return op.replicaHostname
}

// Set the IP address of the new replica.
func (op *addReplicaOperationImpl) SetReplicaHostname(hostname string) {
	op.replicaHostname = hostname
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

// If true, reuse the same SMR node ID for the new node.
func (o *addReplicaWaitOptionsImpl) ReuseSameNodeId() bool {
	return o.reuseSameNodeId
}

// If true, wait for the replica registration to occur.
func (o *addReplicaWaitOptionsImpl) WaitRegistered() bool {
	return o.waitRegistered
}

// If true, wait for the SMR joined notification.
func (o *addReplicaWaitOptionsImpl) WaitSmrJoined() bool {
	return o.waitSmrJoined
}
