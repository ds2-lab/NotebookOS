package scheduler

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

type MigrationArgs struct {
	kernelReplica          scheduling.KernelReplica
	targetHostId           string
	forTraining            bool
	createNewHostPermitted bool
}

// kernelMigrator encapsulates the logic for migrating scheduling.KernelReplica
// instances from one scheduling.Host to another.
type kernelMigrator struct {
	kernelProvider     KernelProvider
	log                logger.Logger
	scheduler          clusterSchedulerInternal
	notificationBroker NotificationBroker
	smrPort            int32

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from TransactionOperation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]]

	// Mapping from new kernel-replica key (i.e., <kernel-id>-<replica-id>) to AddReplicaOperation.
	addReplicaOperationsByKernelReplicaId *hashmap.CornelkMap[string, *scheduling.AddReplicaOperation]

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex
}

func newKernelMigrator(kernelProvider KernelProvider, scheduler clusterSchedulerInternal, smrPort int32) *kernelMigrator {
	return &kernelMigrator{
		kernelProvider:                        kernelProvider,
		scheduler:                             scheduler,
		log:                                   config.GetLogger("KernelMigrator "),
		activeAddReplicaOpsPerKernel:          hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]](64),
		addReplicaOperationsByKernelReplicaId: hashmap.NewCornelkMap[string, *scheduling.AddReplicaOperation](64),
		smrPort:                               smrPort,
	}
}

// MigrateKernelReplica tries to migrate the given kernel to another Host.
//
// The first error that is returned (i.e., 'reason') does not indicate that an actual error occurred.
// It simply provides an explanation for why the migration failed.
//
// The second error that is returned (i.e., 'err') indicates that an actual error occurs.
func (km *kernelMigrator) MigrateKernelReplica(ctx context.Context, args *MigrationArgs) (resp *proto.MigrateKernelResponse, reason error, err error) {
	kernelReplica := args.kernelReplica
	targetHostId := args.targetHostId
	forTraining := args.forTraining

	if kernelReplica == nil {
		km.log.Error("MigrateContainer received nil KernelReplica")
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilKernelReplica
	}

	km.log.Debug("Migrating replica %d of kernel %s. Target host ID: %s. ForTraining=%v.",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHostId, forTraining)

	kernelContainer := kernelReplica.Container()
	if kernelContainer == nil {
		km.log.Error("Cannot migrate replica %d of kernel %s; kernel's kernelContainer is nil",
			kernelReplica.ReplicaID(), kernelReplica.ID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilContainer
	}

	originalHost := kernelContainer.Host()
	if originalHost == nil {
		km.log.Error("Cannot migrate kernelContainer %s. Container's host is nil.", kernelContainer.ContainerID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilOriginalHost
	}

	kernel, loaded := km.kernelProvider.GetKernel(kernelReplica.ID())
	if !loaded {
		km.log.Error("Could not find kernel \"%s\" associated with replica %d being migrated...",
			kernelReplica.ID(), kernelReplica.ReplicaID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId},
			nil, types.ErrKernelNotFound
	}

	//
	// Step 1: Record that migration has started
	//

	// Record that the migration operation is starting.
	err = kernel.MigrationStarted()
	if err != nil {
		km.log.Error("Could not initiate migration of replica %d of kernel \"%s\": %v",
			kernelReplica.ReplicaID(), kernelReplica.ReplicaID(), err)
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, types.ErrKernelNotFound
	}

	defer kernel.MigrationConcluded()

	//
	// Step 2: Find viable target host (or validate provided/specified target host)
	//

	var targetHost scheduling.Host
	targetHost, resp, reason, err = km.validateAndGetTargetHost(args, originalHost)
	if reason != nil || err != nil {
		return resp, reason, err
	}

	km.log.Debug("Found viable migration target for replica %d of kernel %s: host %s",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetNodeName())

	//
	// Step 3: Start new replica on viable target host
	//

	if args.kernelReplica.PersistentID() == "" {
		km.log.Error("Target replica %d of kernel \"%s\" has invalid data directory. Cannot migrate.",
			kernelReplica.ReplicaID(), kernelReplica.ID())

		resp = &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}
		return resp, nil, types.ErrInvalidDataDirectory
	}

	replicaSpec := &proto.ReplicaInfo{
		KernelId:     kernelReplica.ID(),
		ReplicaId:    kernelReplica.ReplicaID(),
		PersistentId: kernelReplica.PersistentID(),
	}

	// AddHost a new replica. We pass "true" for both options (registration and SMR-joining) so we wait
	// for the replica to start fully.
	opts := scheduling.NewAddReplicaWaitOptions(true, true, true)

	var addReplicaOp *scheduling.AddReplicaOperation
	addReplicaOp, err = km.addReplicaDuringMigration(ctx, replicaSpec, targetHost, opts,
		args.kernelReplica.PersistentID(), []scheduling.Host{originalHost}, forTraining)

	// If there's an error here, it's presumably a "real" error, as we already picked out a viable host up above.
	if err != nil {
		km.log.Error("Failed to add new replica %d to kernel %s: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error(
				"Failed to release reservation for replica %d of kernel %s after failing to recreate replica during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v",
				targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	//var newlyAddedReplica scheduling.KernelReplica
	//newlyAddedReplica, err = addReplicaOp.Kernel().GetReplicaByID(addReplicaOp.ReplicaId())
	//if err != nil {
	newlyAddedReplica := addReplicaOp.NewlyAddedReplica
	if newlyAddedReplica == nil {
		km.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v",
			addReplicaOp.ReplicaId(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error("Failed to release reservation for replica %d of kernel %s after not being able to find the replica after supposedly successful migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v",
				targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	km.log.Debug("Successfully created new replica %d of kernel \"%s\" during migration (not done yet)",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	//
	// Step 4: Instruct old replica (the one being migrated) to prepare to migrate by writing state to remote storage
	//

	_, err = km.prepareReplicaForMigration(kernelReplica, originalHost, targetHost)
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	//
	// Step 5: Terminate old replica (the one being migrated)
	//

	err = km.removeTargetReplicaFromHost(kernelReplica, targetHost)
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	//
	// Step 6: Update peer address from old replica to new replica in other replicas of kernel
	//

	if km.scheduler.Policy().NumReplicas() > 1 {
		readyReplica := kernel.GetReadyReplica()
		if readyReplica == nil {
			panic(fmt.Sprintf("Could not find any ready replicas for kernel %s.", kernel.ID()))
		}

		err = km.issueUpdateReplicaRequest(readyReplica, args.kernelReplica.ReplicaID(), newlyAddedReplica.Address())
		if err != nil {
			return &proto.MigrateKernelResponse{
				Id:          -1,
				Hostname:    ErrorHostname,
				NewNodeId:   targetHost.GetID(),
				NewNodeName: targetHost.GetNodeName(),
				Success:     false,
			}, nil, err
		}
	}

	//
	// Step 7: Swap the new replica's container with the old replica's container in the session struct and
	//		   likewise swap the old and new replica in the kernel.
	//

	err = km.performContainerAndKernelReplicaSwap(kernel, newlyAddedReplica, addReplicaOp)
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	//
	// Step 8: Tell new replica to start its Sync Log / Raft Log.
	//
	err = km.issueStartSyncLogRequest(newlyAddedReplica, kernel.PersistentID())
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	//
	// Step 9: Wait for new replica to join its SMR cluster
	//

	km.waitForNewReplicaToJoinSmrCluster(kernel, addReplicaOp, opts)

	km.log.Debug("Designating new replica %d of kernel \"%s\" as \"ready\"",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	// The replica is fully operational at this point, so record that it is ready.
	newlyAddedReplica.SetReady()

	resp = &proto.MigrateKernelResponse{
		Id:          addReplicaOp.ReplicaId(),
		Hostname:    addReplicaOp.ReplicaPodHostname(),
		NewNodeId:   targetHost.GetID(),
		NewNodeName: targetHost.GetNodeName(),
		Success:     true,
	}
	return resp, nil, err
}

// performContainerAndKernelReplicaSwap swaps the new replica's container with the old replica's container in the
// session struct and likewise swap the old and new replica in the kernel.
func (km *kernelMigrator) performContainerAndKernelReplicaSwap(kernel scheduling.Kernel,
	newKernelReplica scheduling.KernelReplica, addReplicaOp *scheduling.AddReplicaOperation) error {

	km.log.Debug("Registering/adding Container for replica %d of kernel %s with the associated Session during migration",
		newKernelReplica.ReplicaID(), addReplicaOp.KernelId())

	session := kernel.GetSession()
	container := newKernelReplica.Container()
	host := newKernelReplica.Host()

	err := session.AddReplica(container)
	if err != nil {
		if errors.Is(err, entity.ErrInvalidContainer) {
			km.log.Error("Error while registering container %v with session %v during migration:\n%v",
				container, session, err)
		} else {
			km.log.Error("Unexpected error while registering container %v with session %v during migration:\n%v",
				container, session, err)
		}

		title := "Failed to Register Container with Session During Migration"
		go km.notificationBroker.SendErrorNotification(title, err.Error())

		return err
	}

	km.log.Debug("Adding replica for kernel %s, replica %d on host %s during migration. Resource spec: %v",
		addReplicaOp.KernelId(), newKernelReplica.ReplicaID(), host.GetID(), newKernelReplica.ResourceSpec())

	err = kernel.AddReplica(newKernelReplica, host)
	if err != nil {
		return err
	}

	return nil
}

// issueStartSyncLogRequest instruct the newly-created scheduling.KernelReplica to start its SyncLog/RaftLog.
func (km *kernelMigrator) issueStartSyncLogRequest(targetReplica scheduling.KernelReplica, persistentId string) error {
	km.log.Debug("↪ issueStartSyncLogRequest: instructing new replica %d of kernel \"%s\" to start its SyncLog.",
		targetReplica.ReplicaID(), targetReplica.ID())

	host := targetReplica.Host()

	if persistentId == "" {
		persistentId = targetReplica.PersistentID()
	}

	_, err := host.StartSyncLog(context.Background(), &proto.ReplicaInfo{
		ReplicaId:    targetReplica.ReplicaID(),
		KernelId:     targetReplica.ID(),
		PersistentId: persistentId,
	})

	if err != nil {
		km.log.Error("↩ issueStartSyncLogRequest: error response from StartSyncLog: %v", err)
		return err
	}

	km.log.Debug("↩ issueStartSyncLogRequest: successfully instructed new replica %d of kernel \"%s\" to start its SyncLog.",
		targetReplica.ReplicaID(), targetReplica.ID())

	return nil
}

// MigrateKernelReplicaOld tries to migrate the given kernel to another Host.
//
// The first error that is returned (i.e., 'reason') does not indicate that an actual error occurred.
// It simply provides an explanation for why the migration failed.
//
// The second error that is returned (i.e., 'err') indicates that an actual error occurs.
func (km *kernelMigrator) MigrateKernelReplicaOld(ctx context.Context, args *MigrationArgs) (resp *proto.MigrateKernelResponse, reason error, err error) {
	kernelReplica := args.kernelReplica
	targetHostId := args.targetHostId
	forTraining := args.forTraining

	if kernelReplica == nil {
		km.log.Error("MigrateContainer received nil KernelReplica")
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilKernelReplica
	}

	km.log.Debug("Migrating replica %d of kernel %s. Target host ID: %s.",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHostId)

	kernelContainer := kernelReplica.Container()
	if kernelContainer == nil {
		km.log.Error("Cannot migrate replica %d of kernel %s; kernel's kernelContainer is nil",
			kernelReplica.ReplicaID(), kernelReplica.ID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilContainer
	}

	originalHost := kernelContainer.Host()
	if originalHost == nil {
		km.log.Error("Cannot migrate kernelContainer %s. Container's host is nil.", kernelContainer.ContainerID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilOriginalHost
	}

	kernel, loaded := km.kernelProvider.GetKernel(kernelReplica.ID())
	if !loaded {
		km.log.Error("Could not find kernel \"%s\" associated with replica %d being migrated...",
			kernelReplica.ID(), kernelReplica.ReplicaID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, types.ErrKernelNotFound
	}

	var targetHost scheduling.Host
	targetHost, resp, reason, err = km.validateAndGetTargetHost(args, originalHost)
	if reason != nil || err != nil {
		return resp, reason, err
	}

	km.log.Debug("Found viable migration target for replica %d of kernel %s: host %s",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetNodeName())

	// Record that the migration operation is starting.
	err = kernel.MigrationStarted()
	if err != nil {
		km.log.Error("Could not initiate migration of replica %d of kernel \"%s\": %v",
			kernelReplica.ReplicaID(), kernelReplica.ReplicaID(), err)
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, types.ErrKernelNotFound
	}

	defer kernel.MigrationConcluded()

	dataDirectory, err := km.prepareReplicaForMigration(kernelReplica, originalHost, targetHost)
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	err = km.removeTargetReplicaFromHost(kernelReplica, targetHost)
	if err != nil {
		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	replicaSpec := &proto.ReplicaInfo{
		KernelId:     kernelReplica.ID(),
		ReplicaId:    kernelReplica.ReplicaID(),
		PersistentId: kernelReplica.PersistentID(),
	}

	// AddHost a new replica. We pass "true" for both options (registration and SMR-joining) so we wait
	// for the replica to start fully.
	opts := scheduling.NewAddReplicaWaitOptions(true, true, true)

	var addReplicaOp *scheduling.AddReplicaOperation
	addReplicaOp, err = km.addReplicaDuringMigration(ctx, replicaSpec, targetHost, opts, dataDirectory,
		[]scheduling.Host{originalHost}, forTraining)

	// If there's an error here, it's presumably a "real" error, as we already picked out a viable host up above.
	if err != nil {
		km.log.Error("Failed to add new replica %d to kernel %s: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error(
				"Failed to release reservation for replica %d of kernel %s after failing to recreate replica during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v",
				targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	km.log.Debug("Successfully added new replica %d of kernel \"%s\" during migration (not quite done yet)",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	var newlyAddedReplica scheduling.KernelReplica
	newlyAddedReplica, err = addReplicaOp.Kernel().GetReplicaByID(addReplicaOp.ReplicaId())
	if err != nil {
		km.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v",
			addReplicaOp.ReplicaId(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error("Failed to release reservation for replica %d of kernel %s after not being able to find the replica after supposedly successful migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v",
				targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	} else {
		km.log.Debug("Successfully added new replica %d to kernel %s during migration operation.",
			addReplicaOp.ReplicaId(), kernelReplica.ID())
	}

	km.log.Debug("Designating new replica %d of kernel \"%s\" as \"ready\"",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	// The replica is fully operational at this point, so record that it is ready.
	newlyAddedReplica.SetReady()

	resp = &proto.MigrateKernelResponse{
		Id:          addReplicaOp.ReplicaId(),
		Hostname:    addReplicaOp.ReplicaPodHostname(),
		NewNodeId:   targetHost.GetID(),
		NewNodeName: targetHost.GetNodeName(),
		Success:     true,
	}
	return resp, nil, err
}

// removeTargetReplicaFromHost terminates the target scheduling.KernelReplica.
func (km *kernelMigrator) removeTargetReplicaFromHost(kernelReplica scheduling.KernelReplica, targetHost scheduling.Host) error {
	err := km.scheduler.RemoveReplicaFromHost(kernelReplica)
	if err != nil {
		km.log.Error("Failed to remove replica %d of kernel %s from its current host: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error("Failed to release reservation for replica %d of kernel %s after failing to remove replica from its current host during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return err
	}

	return nil
}

// prepareReplicaForMigration sends a "prepare to migrate" request to the target replica and returns
// the data directory of the target replica.
func (km *kernelMigrator) prepareReplicaForMigration(kernelReplica scheduling.KernelReplica, originalHost scheduling.Host, targetHost scheduling.Host) (string, error) {
	dataDirectory, err := km.issuePrepareToMigrateRequest(kernelReplica, originalHost)
	if err != nil {
		km.log.Error("Failed to issue 'prepare-to-migrate' request to replica %d of kernel %s: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			km.log.Error("Failed to release reservation for replica %d of kernel %s after failing to issue 'prepare-to-migrate' request during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := km.scheduler.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			km.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return "", err
	}

	return dataDirectory, nil
}

func (km *kernelMigrator) validateAndGetTargetHost(args *MigrationArgs, originalHost scheduling.Host) (targetHost scheduling.Host, resp *proto.MigrateKernelResponse, reason error, err error) {
	// If the caller specified a particular host, then we'll verify that the specified host exists.
	// If it doesn't, then we'll return an error.
	if args.targetHostId != "" {
		targetHost, resp, reason, err = km.validateTargetHost(args)
		if reason != nil || err != nil {
			return nil, resp, reason, err
		}
	}

	// If we weren't already given a target host to migrate the kernel replica to, then let's try to find one now.
	if targetHost == nil {
		// Search for a viable replica, but do not allow the cluster to create a new host/scale out.
		targetHost, reason = km.scheduler.findViableHostForReplica(args.kernelReplica, []scheduling.Host{originalHost},
			args.forTraining, args.createNewHostPermitted)

		if reason != nil || targetHost == nil {
			km.log.Warn("Failed to find a viable host for replica %d of kernel %s: %v",
				args.kernelReplica.ReplicaID(), args.kernelReplica.ID(), reason)
			return nil, &proto.MigrateKernelResponse{
				Id:       -1,
				Hostname: ErrorHostname,
			}, reason, nil
		}
	}

	return targetHost, resp, reason, err
}

func (km *kernelMigrator) validateTargetHost(args *MigrationArgs) (targetHost scheduling.Host, resp *proto.MigrateKernelResponse, reason error, err error) {
	var loaded bool
	targetHost, loaded = km.scheduler.getHost(args.targetHostId)

	if !loaded {
		km.log.Error("Host %s specified as migration target for replica %d of kernel %s; however, host %s does not exist.",
			args.targetHostId, args.kernelReplica.ReplicaID(), args.kernelReplica.ID(), args.targetHostId)
		return nil, &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: args.targetHostId},
			nil, fmt.Errorf("%w: cannot find specified target host %s for migration of replica %d of kernel %s",
				scheduling.ErrHostNotFound, args.targetHostId, args.kernelReplica.ReplicaID(), args.kernelReplica.ID())
	}

	// Make sure that the Host doesn't already have the kernel replica to be migrated or another
	// replica of the same kernel running on it. If so, then the target host is not viable.
	//
	// Likewise, if the host does not have enough resources to serve the kernel replica,
	// then it is not viable.
	if reason = km.isHostViableForMigration(targetHost, args.kernelReplica, args.forTraining); reason != nil {
		return nil, &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   args.targetHostId,
			NewNodeName: targetHost.GetNodeName(),
		}, reason, nil
	}

	return targetHost, nil, nil, nil
}

// isHostViableForMigration returns nil if the specified Host is a viable migration target for the specified
// KernelReplica -- that is, if the specified Host does not already serve another replica of the same kernel, or
// the replica being migrated itself.
//
// Likewise, this also checks that the specified Host has enough resources to serve the specified KernelReplica.
//
// If the Host is not viable, then an ErrHostNotViable error is returned.
func (km *kernelMigrator) isHostViableForMigration(targetHost scheduling.Host, kernelReplica scheduling.KernelReplica, forTraining bool) error {
	if targetHost == nil {
		return scheduling.ErrNilHost
	}

	// If we were able to resolve the host, then let's also verify that the host doesn't already contain
	// another replica of the same kernel (or the replica we're migrating). If so, then the host is not viable.
	existingReplica := targetHost.GetAnyReplicaOfKernel(kernelReplica.ID())
	if existingReplica != nil && existingReplica.ReplicaId() == kernelReplica.ReplicaID() {
		km.log.Warn("Cannot migrate replica %d of kernel %s to host %s. "+
			"Host %s is already hosting replica %d of kernel %s.", kernelReplica.ReplicaID(), kernelReplica.ID(),
			targetHost.GetID(), targetHost.GetID(), existingReplica.ReplicaId(), kernelReplica.ID())

		return fmt.Errorf("%w: replica %d of kernel %s is already running on host %s",
			scheduling.ErrHostNotViable, existingReplica.ReplicaId(), kernelReplica.ID(), targetHost.GetID())
	}

	// Check that there are enough resources available.
	kernelResourceSpec := kernelReplica.ResourceSpec()
	if !targetHost.ResourceSpec().Validate(kernelResourceSpec) {
		km.log.Warn("Cannot migrate replica %d of kernel %s to host %s, as host does not have sufficiently-many allocatable resources to accommodate the replica.",
			kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetID())
		return fmt.Errorf("%w: host lacks sufficiently-many allocatable resourecs", scheduling.ErrHostNotViable)
	}

	// If we're migrating a kernel explicitly to begin training, then we need to see if the target host has sufficient
	// idle resources available.
	if forTraining && !targetHost.CanCommitResources(kernelResourceSpec) {
		km.log.Warn("Cannot migrate replica %d of kernel %s to host %s, as kernel needs to start training, and host lacks sufficient idle resources for this (current idle resources: %v).",
			kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetID(), targetHost.IdleResources().String())
		return fmt.Errorf("%w: insufficient idle resources available for training", scheduling.ErrHostNotViable)
	}

	return nil
}

// issuePrepareMigrateRequest issues a 'prepare-to-migrate' request to a specific replica of a specific kernel.
// This will prompt the kernel to shut down its etcd process (but not remove itself from the cluster)
// before writing the contents of its data directory to intermediate storage.
//
// Returns the path to the data directory in intermediate storage.
func (km *kernelMigrator) issuePrepareToMigrateRequest(kernelReplica scheduling.KernelReplica, originalHost scheduling.Host) (string, error) {
	// If the host is nil, then we'll attempt to retrieve it from the kernel itself.
	if originalHost == nil {
		kernelContainer := kernelReplica.Container()
		if kernelContainer == nil {
			return "", scheduling.ErrNilHost // It's ultimately the host that we need.
		}

		originalHost = kernelContainer.Host()
		if originalHost == nil {
			return "", scheduling.ErrNilHost
		}
	}

	km.log.Debug("Calling PrepareToMigrate RPC targeting host %s (ID=%s) of replica %d of kernel %s now.",
		originalHost.GetNodeName(), originalHost.GetID(), kernelReplica.ReplicaID(), kernelReplica.ID())

	replicaInfo := &proto.ReplicaInfo{
		ReplicaId: kernelReplica.ReplicaID(),
		KernelId:  kernelReplica.ID(),
	}

	resultChan := make(chan interface{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	go func() {
		gRpcClientConnection := originalHost.GetGrpcConnection()

		if gRpcClientConnection == nil {
			err := fmt.Errorf("gRPC Client Connection with host %s (ID=%s) is nil; I hope we're unit-testing",
				originalHost.GetNodeName(), originalHost.GetID())
			km.log.Warn(utils.OrangeStyle.Render(err.Error()))
			// resultChan <- err
		} else {
			km.log.Debug("TransactionState of gRPC ClientConn with host %s (ID=%s): %s (%v)", originalHost.GetNodeName(),
				originalHost.GetID(), gRpcClientConnection.GetState().String(), gRpcClientConnection.GetState())
		}

		// Issue the 'prepare-to-migrate' request. We panic if there was an error.
		resp, err := originalHost.PrepareToMigrate(ctx, replicaInfo)
		if err != nil {
			km.log.Error("Failed to add replica %d of kernel %s to SMR cluster because: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			resultChan <- err
		} else {
			resultChan <- resp
		}
	}()

	var resp *proto.PrepareToMigrateResponse
	select {
	case <-ctx.Done():
		{
			km.log.Error("Timed out waiting for response from host %s (ID=%s) for 'prepare-to-migrate' request for replica %d of kernel %s...",
				originalHost.GetNodeName(), originalHost.GetID(), kernelReplica.ReplicaID(), kernelReplica.ID())
			return "", fmt.Errorf("timed out")
		}
	case res := <-resultChan:
		{
			switch res.(type) {
			case *proto.PrepareToMigrateResponse:
				{
					resp = res.(*proto.PrepareToMigrateResponse)
				}
			case error:
				{
					return "", res.(error)
				}
			}
		}
	}

	dataDirectory := resp.DataDir
	km.log.Debug("Successfully issued 'prepare-to-migrate' request to replica %d of kernel %s on host %s. Data directory: \"%s\"",
		kernelReplica.ReplicaID(), kernelReplica.ID(), originalHost.GetID(), dataDirectory)

	return dataDirectory, nil
}

// AddReplica adds a new replica to a particular distributed kernel.
// This is only used for adding new replicas beyond the base set of replicas created
// when the CloneSet is first created. The first 3 (or however many there are configured
// to be) replicas are created automatically by the CloneSet.
//
// Parameters:
// - kernelId (string): The ID of the kernel to which we're adding a new replica.
// - opts (AddReplicaWaitOptions): Specifies whether we'll wait for registration and/or SMR-joining.
// - dataDirectory (string): Path to etcd-raft data directory in RemoteStorage.
func (km *kernelMigrator) addReplicaDuringMigration(ctx context.Context, in *proto.ReplicaInfo, targetHost scheduling.Host,
	opts scheduling.AddReplicaWaitOptions, dataDirectory string, blacklistedHosts []scheduling.Host,
	forTraining bool) (*scheduling.AddReplicaOperation, error) {

	km.log.Debug("Adding replica %d of kernel \"%s\" during migration operation",
		in.ReplicaId, in.KernelId)

	kernelId := in.KernelId
	persistentId := in.PersistentId

	kernel, ok := km.kernelProvider.GetKernel(kernelId)
	if !ok {
		km.log.Error("Cannot add replica %d to kernel %s: cannot find kernel %s", in.ReplicaId, kernelId, kernelId)
		return nil, types.ErrKernelNotFound
	}

	smrNodeId := int32(-1)

	// Reuse the same SMR node ID if we've been told to do so.
	if opts.ReuseSameNodeId() {
		smrNodeId = in.ReplicaId
	}

	// The spec to be used for the new replica that is created during the migration.
	newReplicaSpec := kernel.PrepareNewReplica(persistentId, smrNodeId)

	km.log.Debug("Officially starting 'add' operation for replica %d of kernel \"%s\" during migration operation",
		newReplicaSpec.ReplicaId, in.KernelId)

	kernel.AddOperationStarted(newReplicaSpec.ReplicaId)

	forMigration := true
	newReplicaSpec.ForMigration = &forMigration

	addReplicaOp := scheduling.NewAddReplicaOperation(kernel, newReplicaSpec, dataDirectory)
	key := fmt.Sprintf("%s-%d", addReplicaOp.KernelId(), addReplicaOp.ReplicaId())
	km.addReplicaOperationsByKernelReplicaId.Store(key, addReplicaOp)

	km.log.Debug("Created new AddReplicaOperation \"%s\": %s", addReplicaOp.OperationID(), addReplicaOp.String())
	km.log.Debug("Adding replica %d to kernel \"%s\" as part of AddReplicaOperation \"%s\" now.",
		newReplicaSpec.ReplicaId, kernelId, addReplicaOp.OperationID())

	// AddHost the AddReplicaOperation to the associated maps belonging to the Gateway Daemon.
	km.addReplicaMutex.Lock()
	ops, ok := km.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		ops = orderedmap.NewOrderedMap[string, *scheduling.AddReplicaOperation]()
	}
	ops.Set(addReplicaOp.OperationID(), addReplicaOp)
	km.activeAddReplicaOpsPerKernel.Store(kernelId, ops)
	km.addReplicaMutex.Unlock()

	km.scheduler.addReplicaSetup(kernelId, addReplicaOp)

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Anything that needs to happen before it's possible for the kernel to have registered already must
	// occur before this line (i.e., before we call ScheduleKernelReplica). Once we call ScheduleKernelReplica,
	// we cannot assume that we've not yet received the registration notification from the kernel, so all of
	// our state needs to be set up BEFORE that call occurs.
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	notifyChan := make(chan interface{}, 1)
	sem := semaphore.NewWeighted(1)
	go func() {
		defer sem.Release(1)

		args := &scheduling.ScheduleReplicaArgs{
			ReplicaSpec:      newReplicaSpec,
			TargetHost:       targetHost,
			BlacklistedHosts: blacklistedHosts,
			ForTraining:      forTraining,
			ForMigration:     true,
		}

		//
		// Create and place the new kernel replica on a host.
		// This is what actually causes the new container to be created.
		//
		err := km.scheduler.ScheduleKernelReplica(ctx, args)

		if err != nil {
			notifyChan <- err
		} else {
			notifyChan <- struct{}{}
		}
	}()

	err := km.waitForNewReplicaToRegister(ctx, in, sem, opts, addReplicaOp, notifyChan)

	if err != nil {
		return nil, err
	}

	// km.waitForNewReplicaToJoinSmrCluster(kernel, addReplicaOp, opts)

	// Return nil on success.
	return addReplicaOp, nil
}

// waitForNewReplicaToRegister waits for the newly-created scheduling.KernelReplica to register with its
// Local Scheduler and subsequently the Cluster Gateway/Cluster Scheduler.
func (km *kernelMigrator) waitForNewReplicaToRegister(ctx context.Context, in *proto.ReplicaInfo, sem *semaphore.Weighted,
	opts scheduling.AddReplicaWaitOptions, addReplicaOp *scheduling.AddReplicaOperation, notifyChan chan interface{}) error {

	if !sem.TryAcquire(1) {
		panic("Failed to acquire on Semaphore")
	}

	// In Kubernetes deployments, the key is the Pod name, which is also the kernel ID + replica suffix.
	// In Docker deployments, the container name isn't really the container's name, but its ID, which is a hash
	// or something like that.
	km.scheduler.postScheduleKernelReplica(in.KernelId, addReplicaOp)

	if opts.WaitRegistered() {
		km.log.Debug("Waiting for new replica %d of kernel \"%s\" to register during AddReplicaOperation \"%s\"",
			addReplicaOp.ReplicaId(), in.KernelId, addReplicaOp.OperationID())
		replicaRegisteredChannel := addReplicaOp.ReplicaRegisteredChannel()

		// We'll keep looping until the call to ScheduleKernelReplica either succeeds, explicitly fails, or times out.
		// We'll also keep looping until the replica has registered.
		var sentBeforeClosed, replicaScheduled, replicaRegistered bool
		for !replicaScheduled || !replicaRegistered {
			select {
			case _, sentBeforeClosed = <-replicaRegisteredChannel:
				{
					if !sentBeforeClosed {
						errorMessage := fmt.Sprintf("Received default value from \"Replica Registered\" channel for AddReplicaOperation \"%s\": %v",
							addReplicaOp.OperationID(), addReplicaOp.String())
						km.log.Error(errorMessage)
						go km.sendErrorNotification("Channel Receive on Closed \"ReplicaRegisteredChannel\" Channel", errorMessage)
					} else {
						addReplicaOp.CloseReplicaRegisteredChannel()
						replicaRegisteredChannel = nil // Prevent infinite loop
					}

					replicaRegistered = true
				}
			case v := <-notifyChan:
				{
					if err, ok := v.(error); ok {
						return err
					}

					replicaScheduled = true
				}
			}
		}

		km.log.Debug("New replica %d of kernel \"%s\" has registered with the Gateway during AddReplicaOperation \"%s\".",
			addReplicaOp.ReplicaId(), in.KernelId, addReplicaOp.OperationID())
	}

	// If we waited for the replica to register, then this will return immediately.
	// Otherwise, we'll be blocked until the call to ScheduleKernelReplica returns (or fails/times out).
	return sem.Acquire(ctx, int64(1))
}

// waitForNewReplicaToJoinSmrCluster blocks until the newly-created scheduling.KernelReplica joins its SMR cluster.
func (km *kernelMigrator) waitForNewReplicaToJoinSmrCluster(kernel scheduling.Kernel,
	addReplicaOp *scheduling.AddReplicaOperation, opts scheduling.AddReplicaWaitOptions) {

	var smrWg sync.WaitGroup
	smrWg.Add(1)

	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted
	// when the new replica joins its SMR cluster.
	go func() {
		km.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster during AddReplicaOperation \"%s\" now...",
			addReplicaOp.ReplicaId(), kernel.ID(), addReplicaOp.OperationID())

		replicaJoinedSmrChannel := addReplicaOp.ReplicaJoinedSmrChannel()
		_, sentBeforeClosed := <-replicaJoinedSmrChannel

		if !sentBeforeClosed {
			errorMessage := fmt.Sprintf("Received default value from \"Replica Joined SMR\" channel for AddReplicaOperation \"%s\": %v",
				addReplicaOp.OperationID(), addReplicaOp.String())
			km.log.Error(errorMessage)

			go km.sendErrorNotification("Channel Receive on Closed \"ReplicaJoinedSmrChannel\" Channel",
				errorMessage)
		}

		close(replicaJoinedSmrChannel)
		km.log.Debug("New replica %d of kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernel.ID())
		kernel.AddOperationCompleted(addReplicaOp.ReplicaId())
		smrWg.Done()

		if !addReplicaOp.Completed() {
			km.log.Error("AddReplicaOperation \"%s\" does not think it's done, even though it should...",
				addReplicaOp.OperationID())

			go km.sendErrorNotification(fmt.Sprintf("AddReplicaOperation \"%s\" is Confused",
				addReplicaOp.OperationID()),
				fmt.Sprintf("AddReplicaOperation \"%s\" does not think it's done, even though it should: %s",
					addReplicaOp.OperationID(), addReplicaOp.String()))
		}
	}()

	if opts.WaitSmrJoined() {
		km.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster...",
			addReplicaOp.ReplicaId(), kernel.ID())
		smrWg.Wait()
	}
}

func (km *kernelMigrator) sendErrorNotification(errorName string, errorMessage string) {
	if km.notificationBroker == nil {
		return
	}

	km.notificationBroker.SendErrorNotification(errorName, errorMessage)
}

func (km *kernelMigrator) sendInfoNotification(title string, message string) {
	if km.notificationBroker == nil {
		return
	}

	km.notificationBroker.SendInfoNotification(title, message)
}

func (km *kernelMigrator) GetActiveAddReplicaOperationsForKernel(kernelId string) (*orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation], bool) {
	return km.activeAddReplicaOpsPerKernel.Load(kernelId)
}

func (km *kernelMigrator) GetAddReplicaOperation(id string) (*scheduling.AddReplicaOperation, bool) {
	return km.addReplicaOperationsByKernelReplicaId.Load(id)
}

func (km *kernelMigrator) GetAddReplicaOperationManager() hashmap.HashMap[string, *scheduling.AddReplicaOperation] {
	return km.addReplicaOperationsByKernelReplicaId
}

// issueUpdateReplicaRequest issues an 'update-replica' request to a replica of a specific kernel, informing that
// replica and its peers that the replica with ID = `nodeId` has a new peer address, namely `newAddress`.
//
// readyReplica is one of the other replicas of the kernel.
func (km *kernelMigrator) issueUpdateReplicaRequest(readyReplica scheduling.KernelReplica, targetReplicaId int32, newAddress string) error {
	km.log.Info("Issuing 'update-replica' request to replica %d of kernel %s for replica %d, newAddr = %s.",
		readyReplica.ReplicaID(), readyReplica.ID(), targetReplicaId, newAddress)

	if !readyReplica.IsReady() {
		panic(fmt.Sprintf("Selected non-ready replica %d of kernel %s to be target of 'update-replica' request...",
			readyReplica.ReplicaID(), readyReplica.ID()))
	}

	host := readyReplica.Host()
	if host == nil {
		panic(fmt.Sprintf("Supposedly 'ready' replica %d of kernel %s does not have a host.",
			readyReplica.ReplicaID(), readyReplica.ID()))
	}

	km.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s to replica %d. Sending request to Local Daemon of replica %d.",
		targetReplicaId, readyReplica.ID(), readyReplica.ReplicaID(), readyReplica.ReplicaID())
	replicaInfo := &proto.ReplicaInfoWithAddr{
		Id:       targetReplicaId,
		KernelId: readyReplica.ID(),
		Hostname: fmt.Sprintf("%s:%d", newAddress, km.smrPort),
	}

	// Issue the 'update-replica' request. We panic if there was an error.
	if _, err := host.UpdateReplicaAddr(context.Background(), replicaInfo); err != nil {
		km.log.Debug("Failed to add replica %d of kernel %s to SMR cluster because: %v", targetReplicaId, readyReplica.ID(), err)
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", targetReplicaId, readyReplica.ID()))
	}

	km.log.Debug("Successfully updated peer address of replica %d of kernel %s to %s.", targetReplicaId, readyReplica.ID(), newAddress)

	return nil
}
