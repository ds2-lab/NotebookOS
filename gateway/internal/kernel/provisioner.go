package kernel

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"log"
	"sync"
)

type Provisioner struct {
	// kernelsStarting is a map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting hashmap.HashMap[string, chan struct{}]

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	cluster scheduling.Cluster

	log logger.Logger
}

func NewProvisioner(cluster scheduling.Cluster) *Provisioner {
	provisioner := &Provisioner{
		cluster: cluster,
	}

	config.InitLogger(&provisioner.log, provisioner)

	return provisioner
}

func (p *Provisioner) SmrReady(in *proto.SmrReadyNotification) error {
	kernelId := in.KernelId

	// First, check if this notification is from a replica of a kernel that is starting up for the very first time.
	// If so, we'll send a notification in the associated channel, and then we'll return.
	kernelStartingChan, ok := p.kernelsStarting.Load(in.KernelId)
	if ok {
		p.log.Debug("Received 'SMR-READY' notification for newly-starting replica %d of kernel %s.",
			in.ReplicaId, in.KernelId)
		kernelStartingChan <- struct{}{}
		return nil
	}

	// Check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	addReplicaOp, ok := p.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, in.ReplicaId)
	if !ok {
		p.log.Warn("Received 'SMR-READY' notification replica %d, kernel %s; however, no add-replica operation found for specified kernel replica...",
			in.ReplicaId, in.KernelId)
		return nil
	}

	if addReplicaOp.Completed() {
		log.Fatalf(utils.RedStyle.Render("Retrieved AddReplicaOperation \"%s\" targeting replica %d of kernel %s -- this operation has already completed.\n"),
			addReplicaOp.OperationID(), in.ReplicaId, kernelId)
	}

	p.log.Debug("Received SMR-READY notification for replica %d of kernel %s [AddOperation.OperationID=%v]. "+
		"Notifying awaiting goroutine now...", in.ReplicaId, kernelId, addReplicaOp.OperationID())
	addReplicaOp.SetReplicaJoinedSMR()

	return nil
}

// Return the add-replica operation associated with the given kernel ID and SMR Node ID of the new replica.
//
// This looks for the most-recently-added AddReplicaOperation associated with the specified replica of the specified kernel.
// If `mustBeActive` is true, then we skip any AddReplicaOperation structs that have already been marked as completed.
func (p *Provisioner) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (*scheduling.AddReplicaOperation, bool) {
	p.addReplicaMutex.Lock()
	defer p.addReplicaMutex.Unlock()

	p.log.Debug("Searching for an active AddReplicaOperation for replica %d of kernel \"%s\".",
		smrNodeId, kernelId)

	activeOps, ok := p.scheduler().GetActiveAddReplicaOperationsForKernel(kernelId)
	if !ok {
		return nil, false
	}

	p.log.Debug("Number of AddReplicaOperation struct(s) associated with kernel \"%s\": %d",
		kernelId, activeOps.Len())

	var op *scheduling.AddReplicaOperation
	// Iterate from newest to oldest, which entails beginning at the back.
	// We want to return the newest AddReplicaOperation that matches the replica ID for this kernel.
	for el := activeOps.Back(); el != nil; el = el.Prev() {
		p.log.Debug("AddReplicaOperation \"%s\": %s", el.Value.OperationID(), el.Value.String())
		// Check that the replica IDs match.
		// If they do match, then we either must not be bothering to check if the operation is still active, or it must still be active.
		if op == nil && el.Value.ReplicaId() == smrNodeId && el.Value.IsActive() {
			op = el.Value
		}
	}

	if op != nil {
		p.log.Debug("Returning AddReplicaOperation \"%s\": %s", op.OperationID(), op.String())
		return op, true
	}

	return nil, false
}

func (p *Provisioner) scheduler() scheduling.Scheduler {
	if p.cluster == nil {
		return nil
	}

	return p.cluster.Scheduler()
}
