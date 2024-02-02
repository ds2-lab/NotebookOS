package daemon

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

type migrationOperationImpl struct {
	id              string                          // Unique identifier of the migration operation.
	kernelId        string                          // ID of the kernel for which a replica is being migrated.
	targetClient    *client.DistributedKernelClient // DistributedKernelClient of the kernel for which we're migrating a replica.
	targetSmrNodeId int32                           // The SMR Node ID of the replica that is being migrated.
	completed       bool                            // True if the migration has been completed; otherwise, false (i.e., if it is still ongoing).
	targetPodName   string                          // Name of the Pod in which the target replica container is running.
}

func NewMigrationOperation(targetClient *client.DistributedKernelClient, targetSmrNodeId int32, targetPodName string) *migrationOperationImpl {
	m := &migrationOperationImpl{
		id:              uuid.New().String(),
		targetClient:    targetClient,
		kernelId:        targetClient.ID(),
		targetSmrNodeId: targetSmrNodeId,
		completed:       false,
		targetPodName:   targetPodName,
	}

	return m
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

// Return true if the migration has been completed; otherwise, return false (i.e., if it is still ongoing).
func (m *migrationOperationImpl) Completed() bool {
	return m.completed
}

// Name of the Pod in which the target replica container is running.
func (m *migrationOperationImpl) TargetPodName() string {
	return m.targetPodName
}

type migrationManagerImpl struct {
	client              KubeClient                                         // The KubeClient that maintains a reference to this migration manager.
	dynamicClient       *dynamic.DynamicClient                             // Own dynamic client, separate from the dynamic client belonging to the associated KubeClient.
	migrationOperations *hashmap.ConcurrentMap[string, MigrationOperation] // Mapping of migration operations.
	kernelMutexes       *hashmap.ConcurrentMap[string, *sync.RWMutex]      // Mapping from Kernel ID to its associated RWMutex.
	// mainMutex           sync.Mutex                                         // Synchronizes certain atomic operations related to internal state and book-keeping of the migration manager.
	log logger.Logger
}

func NewMigrationManager(client KubeClient) *migrationManagerImpl {
	m := &migrationManagerImpl{
		client:              client,
		migrationOperations: hashmap.NewConcurrentMap[MigrationOperation](16),
		kernelMutexes:       hashmap.NewConcurrentMap[*sync.RWMutex](16),
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
	var mu sync.RWMutex
	_, ok := m.kernelMutexes.LoadOrStore(kernelId, &mu) // Atomic.

	if !ok {
		m.log.Debug("Registered kernel \"%s\"", kernelId)
	}
}

// Initiate a migration operation for a particular Pod.
func (m *migrationManagerImpl) MigrateKernelReplica(ctx context.Context, targetClient *client.DistributedKernelClient, targetSmrNodeId int32, in *gateway.ReplicaInfo) {
	podName, err := targetClient.KernelPodName(targetSmrNodeId)
	if err != nil {
		panic(fmt.Sprintf("Could not find replica of kernel \"%s\" with SMR Node ID %d.", targetClient.ID(), targetSmrNodeId))
	}

	migrationOp := NewMigrationOperation(targetClient, in.ReplicaId, podName)
	m.migrationOperations.Store(migrationOp.id, migrationOp)

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

	if retryErr != nil {

	}

	// TODO (Ben): Wait for new Pod to start, assign it the correct SMR Node ID, and then update the CloneSet to have less replicas and delete the correct Pod.

	// return nil, nil
}

// Function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
func (m *migrationManagerImpl) PodCreated(obj interface{}) {
	pod := obj.(*corev1.Pod)
	m.log.Debug("Pod created: %s/%s", pod.Namespace, pod.Name)

	// TODO(Ben): Check if there is an associated migration operation.
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

	// TODO(Ben): Check if there is an associated migration operation.
}
