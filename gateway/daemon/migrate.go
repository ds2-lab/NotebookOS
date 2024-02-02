package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/elliotchance/orderedmap/v2"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/pkg/errors"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

var (
	ErrMigrationOpAlreadyRegistered = errors.New("The given migration operation is already registered as an active operation of the kernel.")
)

type migrationOperationImpl struct {
	id              string                          // Unique identifier of the migration operation.
	kernelId        string                          // ID of the kernel for which a replica is being migrated.
	targetClient    *client.DistributedKernelClient // DistributedKernelClient of the kernel for which we're migrating a replica.
	targetSmrNodeId int32                           // The SMR Node ID of the replica that is being migrated.
	newPodStarted   bool                            // True if a new Pod has been started for the replica that is being migrated. Otherwise, false.
	oldPodStopped   bool                            // True if the original Pod of the replica has stopped. Otherwise, false.
	completed       bool                            // True if the migration has been completed; otherwise, false (i.e., if it is still ongoing).
	oldPodName      string                          // Name of the Pod in which the target replica container is running.
	newPodName      string                          // Name of the new Pod that was started to host the migrated replica.
}

func NewMigrationOperation(targetClient *client.DistributedKernelClient, targetSmrNodeId int32, oldPodName string) *migrationOperationImpl {
	m := &migrationOperationImpl{
		id:              uuid.New().String(),
		targetClient:    targetClient,
		kernelId:        targetClient.ID(),
		targetSmrNodeId: targetSmrNodeId,
		newPodStarted:   false,
		oldPodStopped:   false,
		completed:       false,
		oldPodName:      oldPodName,
	}

	return m
}

func (m *migrationOperationImpl) String() string {
	return fmt.Sprintf("MigrationOperation[ID=%s,KernelID=%s,ReplicaID=%d,Completed=%v,TargetPod=%s]", m.id, m.kernelId, m.targetSmrNodeId, m.completed, m.oldPodName)
}

// Unique identifier of the migration operation.
func (m *migrationOperationImpl) OperationID() string {
	return m.id
}

// DistributedKernelClient of the kernel for which we're migrating a replica.
func (m *migrationOperationImpl) KernelClient() *client.DistributedKernelClient {
	return m.targetClient
}

// The SMR Node ID of the replica that is being migrated.
func (m *migrationOperationImpl) TargetSMRNodeID() int32 {
	return m.targetSmrNodeId
}

// Returns true if a new Pod has been started for the replica that is being migrated. Otherwise, returns false.
func (m *migrationOperationImpl) NewPodStarted() bool {
	return m.newPodStarted
}

// Returns true if the original Pod of the replica has stopped. Otherwise, returns false.
func (m *migrationOperationImpl) OldPodStopped() bool {
	return m.oldPodStopped
}

// Return true if the migration has been completed; otherwise, return false (i.e., if it is still ongoing).
func (m *migrationOperationImpl) Completed() bool {
	return m.completed
}

// Return the name of the Pod in which the target replica container is running.
func (m *migrationOperationImpl) OldPodName() string {
	return m.oldPodName
}

// Return the name of the newly-created Pod that will host the migrated replica.
// Also returns a flag indicating whether the new pod is available. If false, then the returned name is invalid.
func (m *migrationOperationImpl) NewPodName() (string, bool) {
	if m.newPodStarted {
		return m.newPodName, true
	} else {
		return "", false
	}
}

// Set the name of the newly-created Pod that will host the migrated replica. This also records that this operation's new pod has started.
func (m *migrationOperationImpl) SetNewPodName(newPodName string) {
	if m.newPodStarted {
		panic(fmt.Sprintf("Migration operation %s already has a new pod (pod %s).", m.id, m.newPodName))
	}

	m.newPodStarted = true
	m.newPodName = newPodName
}

// Record that the old Pod (containing the replica to be migrated) has stopped.
func (m *migrationOperationImpl) SetOldPodStopped() {
	if m.oldPodStopped {
		panic(fmt.Sprintf("Migration operation %s: old pod (pod %s) already stopped.", m.id, m.oldPodName))
	}

	m.oldPodStopped = true
}

type migrationManagerImpl struct {
	client                     KubeClient                                                                      // The KubeClient that maintains a reference to this migration manager.
	dynamicClient              *dynamic.DynamicClient                                                          // Own dynamic client, separate from the dynamic client belonging to the associated KubeClient.
	migrationOperations        *cmap.ConcurrentMap[string, MigrationOperation]                                 // Mapping of migration operation ID to migration operation.
	activeMigrationOpsPerKenel *cmap.ConcurrentMap[string, *orderedmap.OrderedMap[string, MigrationOperation]] // Mapping of kernel ID to all active migration operations associated with that kernel.
	kernelMutexes              *cmap.ConcurrentMap[string, *sync.Mutex]                                        // Mapping from Kernel ID to its associated RWMutex.
	mainMutex                  sync.Mutex                                                                      // Synchronizes certain atomic operations related to internal state and book-keeping of the migration manager.
	log                        logger.Logger
}

func NewMigrationManager(client KubeClient) *migrationManagerImpl {
	migrationOperations := cmap.New[MigrationOperation]()
	activeMigrationOpsPerKenel := cmap.New[*orderedmap.OrderedMap[string, MigrationOperation]]()
	kernelMutexes := cmap.New[*sync.Mutex]()

	m := &migrationManagerImpl{
		client:                     client,
		migrationOperations:        &migrationOperations,
		kernelMutexes:              &kernelMutexes,
		activeMigrationOpsPerKenel: &activeMigrationOpsPerKenel,
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

	m.dynamicClient = dynamicClient

	return m
}

func (m *migrationManagerImpl) RegisterKernel(kernelId string) {
	var mu sync.Mutex
	set := m.kernelMutexes.SetIfAbsent(kernelId, &mu) // Atomic.

	if !set {
		m.log.Debug("Registered kernel \"%s\"", kernelId)
	}
}

// Initiate a migration operation for a particular Pod.
func (m *migrationManagerImpl) InitiateKernelMigration(ctx context.Context, targetClient *client.DistributedKernelClient, targetSmrNodeId int32, in *gateway.ReplicaInfo) error {
	kernelId := targetClient.ID()
	podName, err := targetClient.KernelPodName(targetSmrNodeId)
	if err != nil {
		panic(fmt.Sprintf("Could not find replica of kernel \"%s\" with SMR Node ID %d.", kernelId, targetSmrNodeId))
	}

	migrationOp := NewMigrationOperation(targetClient, in.ReplicaId, podName)
	m.migrationOperations.Set(migrationOp.id, migrationOp)
	err = m.storeActiveMigrationOperationForKernel(kernelId, migrationOp)
	if err != nil {
		panic(fmt.Sprintf("Migration operation \"%s\" is already registered with kernel \"%s\".", migrationOp.id, kernelId))
	}

	mutex, ok := m.kernelMutexes.Get(kernelId)
	if !ok {
		panic(fmt.Sprintf("Migration Manager does not have a RWMutex registered for kernel \"%s\".", kernelId))
	}

	// Read-lock the mutex so we can safely increment the number of replicas.
	mutex.Lock()

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cloneset_id := fmt.Sprintf("kernel-%s", in.KernelId)
		result, getErr := m.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Get(context.TODO(), cloneset_id, v1.GetOptions{})

		if getErr != nil {
			panic(fmt.Errorf("Failed to get latest version of CloneSet \"%s\": %v", cloneset_id, getErr))
		}

		current_num_replicas, found, err := unstructured.NestedInt64(result.Object, "spec", "replicas")

		if err != nil || !found {
			m.log.Error("Replicas not found for CloneSet %s: error=%s", cloneset_id, err)
			return err
		}

		m.log.Debug("CloneSet \"%s\" is currently configured to have %d replica(s).", cloneset_id, current_num_replicas)

		// Increase the number of replicas.
		if err := unstructured.SetNestedField(result.Object, current_num_replicas+1, "spec", "replicas"); err != nil {
			panic(fmt.Errorf("Failed to set replica value for CloneSet \"%s\": %v", cloneset_id, err))
		}

		_, updateErr := m.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Update(context.TODO(), result, v1.UpdateOptions{})

		if updateErr != nil {
			m.log.Error("Failed to apply update to CloneSet \"%s\": error=%s", cloneset_id, err)
		}

		return updateErr
	})

	// It's possile that the associated unlock should not occur until the associated Pod is created.
	// But we would need to handle a case where the associated Pod cannot be scheduled for some reason,
	// as the lock would be held indefinitely in this case unless we could detect that the Pod was unable to be scheduled.
	// TODO(Ben): Possible race condition if there are concurrent migration operations that increment and decrement the number of replicas concurrently here.
	mutex.Unlock()

	if retryErr != nil {
		return errors.Wrap(retryErr, fmt.Sprintf("Failed to update the CloneSet associated with kernel \"%s\" while migration replica %d.", kernelId, targetSmrNodeId))
	}

	// TODO (Ben): Wait for new Pod to start, assign it the correct SMR Node ID, and then update the CloneSet to have less replicas and delete the correct Pod.
	return nil
}

// Function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodCreated(obj interface{}) {
	pod := obj.(*corev1.Pod)
	m.log.Debug("Pod created: %s/%s", pod.Namespace, pod.Name)

	// TODO(Ben): Check if there is an associated migration operation.

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
		m.log.Error("No 'active migration operations' mapping associated with kernel \"%s\"", kernelId)
	}

	// If there are no active migration operations, then we simply return.
	if activeOps.Len() == 0 {
		return
	}

	mutex, ok := m.kernelMutexes.Get(kernelId)
	if !ok {
		m.log.Error("No mutex found for kernel \"%s\"", kernelId)
	}

	mutex.Lock()

	var op_id string
	var op MigrationOperation
	var validOpFound bool = false
	for el := activeOps.Front(); el != nil; el = el.Next() {
		op_id = el.Key
		op = el.Value

		// The operation has already passed this stage; it's not waiting on a new Pod.
		if op.Completed() || op.NewPodStarted() {
			continue
		}

		m.log.Debug("Found active migration operation \"%s\" for kernel \"%s\" that is waiting on a new Pod to start.", op_id, kernelId)
		validOpFound = true
	}

	if !validOpFound {
		m.log.Warn("Could not find active migration operation for kernel \"%s\" that was waiting on a new Pod to start.", kernelId)
		return
	}

	mutex.Unlock()

	// activeOp, _ := activeOps.Get("")
}

// Function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodUpdated(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*corev1.Pod)
	newPod := newObj.(*corev1.Pod)
	m.log.Debug(
		"Pod updated. %s/%s %s",
		oldPod.Namespace, oldPod.Name, newPod.Status.Phase,
	)
}

// Function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
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
	activeOps, ok := m.activeMigrationOpsPerKenel.Get(kernelId)

	if !ok {
		m.log.Error("No 'active migration operations' mapping associated with kernel \"%s\"", kernelId)
	}

	// If there are no active migration operations, then we simply return.
	if activeOps.Len() == 0 {
		return
	}
}

// Given a kernel ID and a migration operation, register the migration operation as an active migration operation of the kernel identified by the given ID.
// Returns an error if the operation is already registered with the given kernel.
func (m *migrationManagerImpl) storeActiveMigrationOperationForKernel(kernelId string, op *migrationOperationImpl) error {
	m.mainMutex.Lock()
	defer m.mainMutex.Unlock()

	ops_ptr, ok := m.activeMigrationOpsPerKenel.Get(kernelId)

	if !ok {
		ops_ptr = orderedmap.NewOrderedMap[string, MigrationOperation]()
	}

	value_was_new := ops_ptr.Set(op.id, op)
	if !value_was_new {
		return ErrMigrationOpAlreadyRegistered
	}

	m.activeMigrationOpsPerKenel.Set(kernelId, ops_ptr)

	return nil
}
