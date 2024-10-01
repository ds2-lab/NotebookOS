package daemon

import (
	"context"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"strings"
	"sync"

	"github.com/elliotchance/orderedmap/v2"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/pkg/errors"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

var (
	ErrMigrationOpAlreadyRegistered = errors.New("The given migration operation is already registered as an active operation of the kernel.")
	ErrMigrationOpNotFound          = errors.New("The given migration operation was not in the list and thus could not be deleted (as it wasn't present to begin with).")
)

type BasicMigrationOperation struct {
	id                   string                          // Unique identifier of the migration operation.
	kernelId             string                          // ID of the kernel for which a replica is being migrated.
	targetClient         *client.DistributedKernelClient // distributedKernelClientImpl of the kernel for which we're migrating a replica.
	targetSmrNodeId      int32                           // The SMR Node ID of the replica that is being migrated.
	newPodStarted        bool                            // True if a new Pod has been started for the replica that is being migrated. Otherwise, false.
	newReplicaJoinedSMR  bool                            // True if the new replica has joined the SMR cluster. Otherwise, false.
	oldPodStopped        bool                            // True if the original Pod of the replica has stopped. Otherwise, false.
	oldPodName           string                          // Name of the Pod in which the target replica container is running.
	newPodName           string                          // Name of the new Pod that was started to host the migrated replica.
	newReplicaRegistered bool                            // If true, then new replica has registered with the Gateway.
	persistentId         string                          // Persistent ID of replica.
	newReplicaHostname   string                          // The IP address of the new replica.
	newSpec              *proto.KernelReplicaSpec        // Spec for the new replica that is created during the migration.

	// podStartedMu   sync.Mutex // Used to signal that the new Pod has started.
	// podStartedCond *sync.Cond // Used with the podStartedCond condition variable.

	// podStoppedMu   sync.Mutex // Used to signal that the old Pod has stopped.
	// podStoppedCond *sync.Cond // Used with the podStoppedCond condition variable.

	opCompletedMu   sync.Mutex // Used with the opCompletedCond condition variable.
	opCompletedCond *sync.Cond // Used to signal that the Migration has completed.

	// completed       bool                            // True if the migration has been completed; otherwise, false (i.e., if it is still ongoing).
}

func NewMigrationOperation(targetClient *client.DistributedKernelClient, targetSmrNodeId int32, oldPodName string, newSpec *proto.KernelReplicaSpec) *BasicMigrationOperation {
	m := &BasicMigrationOperation{
		id:                   uuid.New().String(),
		targetClient:         targetClient,
		kernelId:             targetClient.ID(),
		targetSmrNodeId:      targetSmrNodeId,
		newPodStarted:        false,
		newReplicaJoinedSMR:  false,
		newReplicaRegistered: false,
		oldPodStopped:        false,
		persistentId:         *newSpec.PersistentId,
		oldPodName:           oldPodName,
		newSpec:              newSpec,
		// completed:       false,
	}

	m.opCompletedCond = sync.NewCond(&m.opCompletedMu)

	return m
}

// GetNewReplicaRegistered returns true if the new replica has already registered with the Gateway; otherwise, return false.
func (m *BasicMigrationOperation) GetNewReplicaRegistered() bool {
	return m.newReplicaRegistered
}

// GetNewReplicaKernelSpec returns the *gateway.KernelReplicaSpec for the new replica that is created during the migration.
func (m *BasicMigrationOperation) GetNewReplicaKernelSpec() *proto.KernelReplicaSpec {
	return m.newSpec
}

// NewReplicaHostname returns the IP address of the new replica.
func (m *BasicMigrationOperation) NewReplicaHostname() string {
	return m.newReplicaHostname
}

// SetNewReplicaHostname sets the IP address of the new replica.
func (m *BasicMigrationOperation) SetNewReplicaHostname(hostname string) {
	m.newReplicaHostname = hostname
}

// NotifyNewReplicaRegistered records that the new replica for this migration operation has registered with the Gateway.
// Will panic if we've already recorded that the new replica has registered.
func (m *BasicMigrationOperation) NotifyNewReplicaRegistered() {
	if m.newReplicaRegistered {
		panic(fmt.Sprintf("We've already recorded that new replica has registered for %v.", m.String()))
	}

	m.newReplicaRegistered = true
}

func (m *BasicMigrationOperation) String() string {
	return fmt.Sprintf("domain.MigrationOperation[ID=%s,KernelID=%s,ReplicaID=%d,Completed=%v,TargetPod=%s,NewPodName=%s,PersistentID=%s,NewReplicaRegistered=%v]", m.id, m.kernelId, m.targetSmrNodeId, m.Completed(), m.oldPodName, m.newPodName, m.persistentId, m.newReplicaRegistered)
}

// OperationID returns the unique identifier of the migration operation.
func (m *BasicMigrationOperation) OperationID() string {
	return m.id
}

// KernelReplicaClient returns the client.DistributedKernelClient of the kernel for which we're migrating a replica.
func (m *BasicMigrationOperation) KernelReplicaClient() *client.DistributedKernelClient {
	return m.targetClient
}

// OriginalSMRNodeID returns the SMR Node ID of the replica that is being migrated.
func (m *BasicMigrationOperation) OriginalSMRNodeID() int32 {
	return m.targetSmrNodeId
}

// NewReplicaJoinedSMR returns true if a new Pod has been started for the replica that is being migrated. Otherwise, returns false.
func (m *BasicMigrationOperation) NewReplicaJoinedSMR() bool {
	return m.newReplicaJoinedSMR
}

// SetNewReplicaJoinedSMR records that the new replica has joined its SMR cluster.
func (m *BasicMigrationOperation) SetNewReplicaJoinedSMR() {
	m.newReplicaJoinedSMR = true
}

// NewPodStarted returns true if the new Pod has started.
func (m *BasicMigrationOperation) NewPodStarted() bool {
	return m.newPodStarted
}

// OldPodStopped returns true if the original Pod of the replica has stopped. Otherwise, returns false.
func (m *BasicMigrationOperation) OldPodStopped() bool {
	return m.oldPodStopped
}

// Completed returns true if the migration has been completed; otherwise, return false (i.e., if it is still ongoing).
func (m *BasicMigrationOperation) Completed() bool {
	return m.oldPodStopped && m.newPodStarted && m.newReplicaRegistered && m.newReplicaJoinedSMR
}

// OldPodName returns the name of the Pod in which the target replica container is running.
func (m *BasicMigrationOperation) OldPodName() string {
	return m.oldPodName
}

// NewPodName returns the name of the newly-created Pod that will host the migrated replica.
// Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
func (m *BasicMigrationOperation) NewPodName() (string, bool) {
	if m.newPodStarted {
		return m.newPodName, true
	} else {
		return "", false
	}
}

// SetNewPodName sets the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
func (m *BasicMigrationOperation) SetNewPodName(newPodName string) {
	if m.newPodStarted {
		panic(fmt.Sprintf("Migration operation %s already has a new pod (pod %s).", m.id, m.newPodName))
	}

	m.newPodStarted = true
	m.newPodName = newPodName
}

// SetOldPodStopped records that the old Pod (containing the replica to be migrated) has stopped.
func (m *BasicMigrationOperation) SetOldPodStopped() {
	if m.oldPodStopped {
		panic(fmt.Sprintf("Migration operation %s: old pod (pod %s) already stopped.", m.id, m.oldPodName))
	}

	m.oldPodStopped = true
}

// Wait blocks and waits until the migration operation has completed.
func (m *BasicMigrationOperation) Wait() {
	m.opCompletedCond.L.Lock()
	m.opCompletedCond.Wait()
	m.opCompletedCond.L.Unlock()
}

// Broadcast notifies any go routines waiting for the migration operation to complete. Should only be called once the migration operation has completed.
func (m *BasicMigrationOperation) Broadcast() {
	m.opCompletedCond.L.Lock()
	m.opCompletedCond.Broadcast()
	m.opCompletedCond.L.Unlock()
}

func (m *BasicMigrationOperation) PersistentID() string {
	return m.persistentId
}

// KernelId returns the kernel ID.
func (m *BasicMigrationOperation) KernelId() string {
	return m.kernelId
}

// Note on the use of orderedmap.OrderedMap.
// We use an ordered map to store the active migration operations for each kernel so that operations that were initiated first are completed/processed first.
type migrationManagerImpl struct {
	dynamicClient                   *dynamic.DynamicClient                                                                 // Own dynamic client, separate from the dynamic client belonging to the associated KubeClient.
	kubeClientset                   *kubernetes.Clientset                                                                  // Clientset contains the clients for groups. Each group has exactly one version included in a Clientset.
	migrationOperations             *cmap.ConcurrentMap[string, domain.MigrationOperation]                                 // Mapping of migration operation ID to migration operation.
	migrationOperationsByOldPodName *cmap.ConcurrentMap[string, domain.MigrationOperation]                                 // Mapping of old Pod names to their associated migration operation.
	migrationOperationsByNewPodName *cmap.ConcurrentMap[string, domain.MigrationOperation]                                 // Mapping of new Pod names to their associated migration operation.
	newPodWaiters                   *cmap.ConcurrentMap[string, chan domain.MigrationOperation]                            // Mapping of new Pod names to channels. Used by the Gateway Daemon to wait until we receive a pod-created notification during migrations.
	activeMigrationOpsPerKenel      *cmap.ConcurrentMap[string, *orderedmap.OrderedMap[string, domain.MigrationOperation]] // Mapping of kernel ID to all active migration operations associated with that kernel. The inner maps are from Operation ID to domain.MigrationOperation.
	kernelMutexes                   *cmap.ConcurrentMap[string, *sync.Mutex]                                               // Mapping from Kernel ID to its associated RWMutex.
	mainMutex                       sync.Mutex                                                                             // Synchronizes certain atomic operations related to internal state and book-keeping of the migration manager.
	log                             logger.Logger
}

func NewMigrationManager() *migrationManagerImpl {
	migrationOperations := cmap.New[domain.MigrationOperation]()
	migrationOperationsByOldPodName := cmap.New[domain.MigrationOperation]()
	migrationOperationsByNewPodName := cmap.New[domain.MigrationOperation]()
	newPodWaiters := cmap.New[chan domain.MigrationOperation]()
	activeMigrationOpsPerKenel := cmap.New[*orderedmap.OrderedMap[string, domain.MigrationOperation]]()
	kernelMutexes := cmap.New[*sync.Mutex]()

	m := &migrationManagerImpl{
		migrationOperations:             &migrationOperations,
		kernelMutexes:                   &kernelMutexes,
		activeMigrationOpsPerKenel:      &activeMigrationOpsPerKenel,
		migrationOperationsByOldPodName: &migrationOperationsByOldPodName,
		migrationOperationsByNewPodName: &migrationOperationsByNewPodName,
		newPodWaiters:                   &newPodWaiters,
	}

	config.InitLogger(&m.log, m)

	dynamicConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Create the "Dynamic" client, which is used for unstructured components, such as CloneSets.
	dynamicClient, err := dynamic.NewForConfig(dynamicConfig)
	if err != nil {
		panic(err.Error())
	}

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(err.Error())
	}

	m.dynamicClient = dynamicClient
	m.kubeClientset = clientset

	return m
}

func (m *migrationManagerImpl) RegisterKernel(kernelId string) {
	var mu sync.Mutex
	set := m.kernelMutexes.SetIfAbsent(kernelId, &mu) // Atomic.

	if !set {
		m.log.Debug("Registered kernel \"%s\"", kernelId)
	}
}

// InitiateKernelMigration initiates a migration operation for a particular Pod.
func (m *migrationManagerImpl) InitiateKernelMigration(_ context.Context, targetClient *client.DistributedKernelClient, targetSmrNodeId int32, newSpec *proto.KernelReplicaSpec) (string, error) {
	kernelId := targetClient.ID()
	podName, err := targetClient.PodName(targetSmrNodeId)
	if err != nil {
		panic(fmt.Sprintf("Could not find replica of kernel \"%s\" with SMR Node ID %d.", kernelId, targetSmrNodeId))
	}

	targetClient.AddOperationStarted()
	migrationOp := NewMigrationOperation(targetClient, targetSmrNodeId, podName, newSpec)

	// Store the migration operation in some maps.
	m.mainMutex.Lock()
	m.migrationOperations.Set(migrationOp.id, migrationOp)
	m.migrationOperationsByOldPodName.Set(podName, migrationOp)
	err = m.storeActiveMigrationOperationForKernel(kernelId, migrationOp)
	m.mainMutex.Unlock()

	m.log.Warn("Initiating kernel replica migration \"%s\" for kernel %s, targeting replica %d. Old pod name: \"%s\"", migrationOp.OperationID(), kernelId, targetSmrNodeId, podName)

	if err != nil {
		panic(fmt.Sprintf("Migration operation \"%s\" is already registered with kernel \"%s\".", migrationOp.id, kernelId))
	}

	mutex, ok := m.kernelMutexes.Get(kernelId)
	if !ok {
		panic(fmt.Sprintf("Migration Manager does not have a RWMutex registered for kernel \"%s\".", kernelId))
	}

	// Read-lock the mutex so we can safely increment the number of replicas.
	mutex.Lock()

	// Increase the number of replicas.
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		clonesetId := fmt.Sprintf("kernel-%s", kernelId)
		result, getErr := m.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Get(context.TODO(), clonesetId, metav1.GetOptions{})

		if getErr != nil {
			panic(fmt.Errorf("failed to get latest version of CloneSet \"%s\": %v", clonesetId, getErr))
		}

		currentNumReplicas, found, err := unstructured.NestedInt64(result.Object, "spec", "replicas")

		if err != nil || !found {
			m.log.Error("Replicas not found for CloneSet %s: error=%s", clonesetId, err)
			return err
		}

		m.log.Debug("Attempting to INCREASE the number of replicas of CloneSet \"%s\". Currently, it is configured to have %d replicas.", clonesetId, currentNumReplicas)
		newNumReplicas := currentNumReplicas + 1

		// Increase the number of replicas.
		if err := unstructured.SetNestedField(result.Object, newNumReplicas, "spec", "replicas"); err != nil {
			panic(fmt.Errorf("failed to set replica value for CloneSet \"%s\": %v", clonesetId, err))
		}

		_, updateErr := m.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Update(context.TODO(), result, metav1.UpdateOptions{})

		if updateErr != nil {
			m.log.Error("Failed to apply update to CloneSet \"%s\": error=%s", clonesetId, err)
		} else {
			m.log.Debug("Successfully increased number of replicas of CloneSet \"%s\" to %d.", clonesetId, newNumReplicas)
		}

		return updateErr
	})

	// It's possible that the associated unlock should not occur until the associated Pod is created.
	// But we would need to handle a case where the associated Pod cannot be scheduled for some reason,
	// as the lock would be held indefinitely in this case unless we could detect that the Pod was unable to be scheduled.
	// TODO(Ben): Possible race condition if there are concurrent migration operations that increment and decrement the number of replicas concurrently here.
	mutex.Unlock()

	if retryErr != nil {
		return "", errors.Wrap(retryErr, fmt.Sprintf("Failed to update the CloneSet associated with kernel \"%s\" while migration replica %d.", kernelId, targetSmrNodeId))
	}

	m.log.Debug("Waiting for Migration Operation %s on replica %d of kernel %s to complete before returning.", migrationOp.OperationID(), targetSmrNodeId, kernelId)
	migrationOp.Wait()
	m.log.Info("Migration Operation %s on replica %d of kernel %s to completed. Returning now.", migrationOp.OperationID(), targetSmrNodeId, kernelId)

	// TODO (Ben): Wait for new Pod to start, assign it the correct SMR Node ID, and then update the CloneSet to have less replicas and delete the correct Pod.
	return migrationOp.NewReplicaHostname(), nil
}

// Wait for us to receive a pod-created notification for the given Pod, which managed to start running
// and register with us before we received the pod-created notification. Once received, return the
// associated migration operation.
// func (m *migrationManagerImpl) WaitForNewPodNotification(newPodName string) domain.MigrationOperation {
// 	m.mainMutex.Lock()

// 	// First, try to get the migration operation, in case we received the notification since the time we made the call to WaitForNewPodNotification.
// 	op, ok := m.migrationOperationsByNewPodName.Get(newPodName)
// 	if ok {
// 		m.mainMutex.Unlock()
// 		return op
// 	}

// 	_ = m.newPodWaiters.SetIfAbsent(newPodName, make(chan domain.MigrationOperation))
// 	channel, _ := m.newPodWaiters.Get(newPodName)

// 	m.mainMutex.Unlock()

// 	m.log.Debug("Waiting on channel for pod-created notification for new pod \"%s\"", newPodName)
// 	select {
// 	case op := <-channel:
// 		{
// 			return op
// 		}
// 	}
// }

// PodCreated is a function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodCreated(obj interface{}) {
	pod := obj.(*corev1.Pod)
	m.log.Debug("Pod created: %s/%s", pod.Namespace, pod.Name)

	// First, check if the newly-created Pod is a kernel Pod.
	// If it is not a kernel Pod, then we simply return.
	if !strings.HasPrefix(pod.Name, "kernel") {
		return
	}

	// Next, check if there is an associated migration operation with the Pod
	// Example Pod name:
	// kernel-5aef36f7-ae8b-477c-9162-178f2d4b85df-ABCDE
	kernelId := pod.Name[7:43]
	activeOps, ok := m.activeMigrationOpsPerKenel.Get(kernelId)

	if !ok {
		// No migration operation associated with this kernel, so we just return.
		return
	}

	// If there are no active migration operations, then we simply return.
	if activeOps.Len() == 0 {
		return
	}

	mutex, ok := m.kernelMutexes.Get(kernelId)
	if !ok {
		panic(fmt.Sprintf("No mutex found for kernel \"%s\"", kernelId))
	}

	mutex.Lock()

	var opId string
	var op domain.MigrationOperation
	var validOpFound = false
	for el := activeOps.Front(); el != nil; el = el.Next() {
		opId = el.Key
		op = el.Value

		// The operation has already passed this stage; it's not waiting on a new Pod.
		if op.Completed() || op.NewPodStarted() {
			continue
		}

		m.log.Debug("Found active migration operation \"%s\" for kernel \"%s\" that is waiting on a new Pod to start.", opId, kernelId)
		validOpFound = true
	}

	if op == nil || !validOpFound {
		m.log.Warn("Could not find active migration operation for kernel \"%s\" that was waiting on a new Pod to start.", kernelId)
		return
	}

	op.SetNewPodName(pod.Name)
	m.migrationOperationsByNewPodName.Set(pod.Name, op)

	// // Label the Pod that we would like to delete so that the CloneSet prioritizes deleting it when we scale it down in the next step.
	// err := m.addKruiseDeleteLabelToPod(op.OldPodName(), "default")
	// if err != nil {
	// 	panic(err)
	// }
	// // Decrease the number of replicas of the CloneSet. The Pod that we labeled in the previous step should be deleted.
	// err = m.scaleDownCloneSet(op)
	// if err != nil {
	// 	panic(err)
	// }

	mutex.Unlock()

	m.mainMutex.Lock()
	defer m.mainMutex.Unlock()

	channel, ok := m.newPodWaiters.Get(pod.Name)

	// If there's a goroutine waiting for this pod-created notification to be received, which is determined simply by the existence of an entry
	// in the `newPodWaiters` map for the new pod's name as the key, then send the migration operation over the channel to the waiting goroutine.
	if ok {
		m.log.Debug("Sending migration operation %s thru new-pod channel for new pod %s", op.OperationID(), pod.Name)
		channel <- op
	}

	// TODO(Ben):
	// We need to be careful here so as not to step on any other concurrent migration operations.
	// We also need to facilitate hooking up the new replica with the rest of the system.
}

// PodUpdated is a function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodUpdated(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*corev1.Pod)
	newPod := newObj.(*corev1.Pod)
	if newPod.Status.Phase == corev1.PodFailed {
		m.log.Error(
			"Pod updated. %s/%s %s",
			oldPod.Namespace, oldPod.Name, newPod.Status.Phase)
	}
}

// PodDeleted is a function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodDeleted(obj interface{}) {
	pod := obj.(*corev1.Pod)
	m.log.Debug("Pod deleted: %s/%s", pod.Namespace, pod.Name)

	// First, check if the newly-created Pod is a kernel Pod.
	// If it is not a kernel Pod, then we simply return.
	if !strings.HasPrefix(pod.Name, "kernel") {
		return
	}

	// Next, check if there is an associated migration operation with the Pod
	// Example Pod name:
	// kernel-5aef36f7-ae8b-477c-9162-178f2d4b85df-ABCDE
	kernelId := pod.Name[7:43]

	activeOps, foundActiveOps := m.activeMigrationOpsPerKenel.Get(kernelId)
	op, ok := m.migrationOperationsByOldPodName.Get(pod.Name)

	// No operation found when looking by old Pod name, so we can just return.
	if !ok {
		// If we either couldn't find any active migration operations, or there are none, then just return.
		if !foundActiveOps || activeOps.Len() == 0 {
			m.log.Debug("No active migration operations found for kernel %s of deleted Pod %s.", kernelId, pod.Name)
			return
		}

		op, ok = activeOps.Get(kernelId)

		if !ok {
			m.log.Warn("Could not find active migration operation associated with old, now-deleted pod %s.", pod.Name)
			return
		}
	}

	// At this point, we know we found an operation by old Pod name.
	// So, if there are no active operations, then we're in an error state.
	if !foundActiveOps || activeOps.Len() == 0 {
		panic(fmt.Sprintf("Found migration operation %s by old pod name %s, but no active ops found.", op.OperationID(), pod.Name))
	}

	// Sanity check. The op we found via old pod Name should be in the active operations map.
	if _, ok = activeOps.Get(op.OperationID()); !ok {
		panic(fmt.Sprintf("Found migration operation %s by old pod name %s, but it is not included in the active operations for the associated kernel.", op.OperationID(), pod.Name))
	}

	m.log.Debug("Recording that old pod %s stopped for active migration operation %s.", pod.Name, op.OperationID())
	op.SetOldPodStopped()

	if !op.NewPodStarted() {
		panic(fmt.Sprintf("Old pod \"%s\" stopped for Migration Operation %s for Kernel %s, but new Pod has not yet started.", op.OldPodName(), op.OperationID(), op.KernelId()))
	}

	// Check if we're done. We'll be done when both the old Pod has stopped AND when the new replica has joined its SMR cluster.
	m.CheckIfMigrationCompleted(op)
}

// Called by CheckIfMigrationCompleted when a migration operation has completed successfully.
// IMPORTANT: The main mutex MUST be held when this is called.
func (m *migrationManagerImpl) migrationCompleted(op domain.MigrationOperation) {
	m.log.Debug("Migration %s of replica %d of kernel %s completed successfully.", op.OperationID(), op.OriginalSMRNodeID(), op.KernelId())

	op.KernelReplicaClient().AddOperationCompleted()
	// Wake up anybody waiting.
	op.Broadcast()

	err := m.removeActiveMigrationOperationForKernel(op.KernelId(), op)
	if err != nil {
		m.log.Error("Error encountered while deleting migration operation %s from active operations of kernel %s: %v", op.OperationID(), op.KernelId(), err)
	}
}

// CheckIfMigrationCompleted checks if the given Migration Operation has finished. This is called twice: when the
// new replica registers with the Gateway, and when the old Pod is deleted. Whichever of those two events happens
// last will be the one that designates the operation has having completed.
func (m *migrationManagerImpl) CheckIfMigrationCompleted(op domain.MigrationOperation) bool {
	m.mainMutex.Lock()
	defer m.mainMutex.Unlock()
	if op.Completed() {
		m.migrationCompleted(op) // Need to have the lock when we call this.
		return true
	}

	return false
}

func (m *migrationManagerImpl) GetMigrationOperationByNewPod(newPodName string) (domain.MigrationOperation, bool) {
	return m.migrationOperationsByNewPodName.Get(newPodName)
}

// GetMigrationOperationByKernelIdAndNewReplicaId returns the migration operation associated with the given
// Kernel ID and SMR Node ID of the new replica.
func (m *migrationManagerImpl) GetMigrationOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (domain.MigrationOperation, bool) {
	m.mainMutex.Lock()
	defer m.mainMutex.Unlock()

	activeOps, ok := m.activeMigrationOpsPerKenel.Get(kernelId)
	if !ok {
		return nil, false
	}

	var op domain.MigrationOperation
	for el := activeOps.Front(); el != nil; el = el.Next() {
		op = el.Value

		if op.GetNewReplicaKernelSpec().ReplicaId == smrNodeId {
			return op, true
		}
	}

	return nil, false
}

// Given a kernel ID and a migration operation, register the migration operation as an active migration operation of the kernel identified by the given ID.
// Returns an error if the operation is already registered with the given kernel.
//
// Note: this MUST be called with the main mutex held!
func (m *migrationManagerImpl) storeActiveMigrationOperationForKernel(kernelId string, op domain.MigrationOperation) error {
	opsPtr, ok := m.activeMigrationOpsPerKenel.Get(kernelId)

	if !ok {
		opsPtr = orderedmap.NewOrderedMap[string, domain.MigrationOperation]()
	}

	valueWasNew := opsPtr.Set(op.OperationID(), op)
	if !valueWasNew {
		return ErrMigrationOpAlreadyRegistered
	}

	m.activeMigrationOpsPerKenel.Set(kernelId, opsPtr)

	return nil
}

func (m *migrationManagerImpl) removeActiveMigrationOperationForKernel(kernelId string, op domain.MigrationOperation) error {
	opsPtr, ok := m.activeMigrationOpsPerKenel.Get(kernelId)

	if !ok {
		opsPtr = orderedmap.NewOrderedMap[string, domain.MigrationOperation]()
	}

	didDelete := opsPtr.Delete(op.OperationID())
	if !didDelete {
		return ErrMigrationOpNotFound
	}

	m.activeMigrationOpsPerKenel.Set(kernelId, opsPtr)

	return nil
}
