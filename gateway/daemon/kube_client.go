package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/pkg/errors"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/retry"
)

const (
	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/kernel-configmap"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"

	IPythonConfigPath        = "IPYTHON_CONFIG_PATH"
	IPythonConfigPathDefault = "/home/jovyan/.ipython/profile_default/ipython_config.json"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080

	DummySMRNodeId = -987654321
)

var (
	kubeStorageBase = "/storage"                                                                                       // TODO(Ben): Don't hard-code this. What should this be?
	clonesetRes     = schema.GroupVersionResource{Group: "apps.kruise.io", Version: "v1alpha1", Resource: "clonesets"} // Identifier for Kubernetes CloneSet resources.

	ErrNodeNotFound = errors.New("could not find kubernetes node with the specified name")
)

type BasicKubeClient struct {
	kubeClientset          *kubernetes.Clientset                      // Clientset contains the clients for groups. Each group has exactly one version included in a Clientset.
	dynamicClient          *dynamic.DynamicClient                     // Dynamic client for working with unstructured components. We use this for the custom CloneSet.
	gatewayDaemon          scheduling.ClusterGateway                  // Associated Gateway daemon.
	configDir              string                                     // Where to write config files. This is also where they'll be found on the kernel nodes.
	ipythonConfigPath      string                                     // Where the IPython config is located.
	nodeLocalMountPoint    string                                     // The mount of the shared PVC for all kernel nodes.
	localDaemonServiceName string                                     // Name of the service controlling the routing of the local daemon. It only routes traffic on the same node.
	localDaemonServicePort int                                        // Port that local daemon service will be routing traffic to.
	smrPort                int                                        // Port used for the SMR protocol.
	kubeNamespace          string                                     // Kubernetes namespace that all of these components reside in.
	useStatefulSet         bool                                       // If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet.
	podWatcherStopChan     chan struct{}                              // Used to tell the Pod Watcher to stop.
	mutex                  sync.Mutex                                 // Synchronize atomic operations, such as scaling-up/down a CloneSet.
	scaleUpChannels        *cmap.ConcurrentMap[string, []chan string] // Mapping from Kernel ID to a slice of channels, each of which would correspond to a scale-up operation.
	scaleDownChannels      *cmap.ConcurrentMap[string, chan struct{}] // Mapping from Pod name a channel, each of which would correspond to a scale-down operation.
	hdfsNameNodeEndpoint   string                                     // Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.
	schedulingPolicy       string                                     // Scheduling policy.
	notebookImageName      string                                     // Name of the docker image to use for the jupyter notebook/kernel image
	notebookImageTag       string                                     // Tag to use for the jupyter notebook/kernel image
	checkpointingEnabled   bool                                       // checkpointingEnabled controls whether the kernels should perform checkpointing after a migration and after executing code.
	log                    logger.Logger
}

func NewKubeClient(gatewayDaemon scheduling.ClusterGateway, clusterDaemonOptions *domain.ClusterDaemonOptions) *BasicKubeClient {
	scaleUpChannels := cmap.New[[]chan string]()
	scaleDownChannels := cmap.New[chan struct{}]()

	client := &BasicKubeClient{
		configDir:              utils.GetEnv(KubeSharedConfigDir, KubeSharedConfigDirDefault),
		ipythonConfigPath:      utils.GetEnv(IPythonConfigPath, IPythonConfigPathDefault),
		nodeLocalMountPoint:    utils.GetEnv(KubeNodeLocalMountPoint, KubeNodeLocalMountPointDefault),
		localDaemonServiceName: clusterDaemonOptions.LocalDaemonServiceName,
		localDaemonServicePort: clusterDaemonOptions.LocalDaemonServicePort,
		smrPort:                clusterDaemonOptions.SMRPort,
		kubeNamespace:          clusterDaemonOptions.KubeNamespace,
		gatewayDaemon:          gatewayDaemon,
		useStatefulSet:         clusterDaemonOptions.UseStatefulSet,
		scaleUpChannels:        &scaleUpChannels,
		scaleDownChannels:      &scaleDownChannels,
		podWatcherStopChan:     make(chan struct{}),
		hdfsNameNodeEndpoint:   clusterDaemonOptions.HdfsNameNodeEndpoint,
		notebookImageName:      clusterDaemonOptions.NotebookImageName,
		notebookImageTag:       clusterDaemonOptions.NotebookImageTag,
		checkpointingEnabled:   clusterDaemonOptions.SimulateCheckpointingLatency,
	}

	if client.hdfsNameNodeEndpoint == "" {
		panic("The HDFS NameNode endpoint cannot be \"\"")
	}

	config.InitLogger(&client.log, client)

	if clusterDaemonOptions.IsLocalMode() {
		var kubeconfigPath string
		home := homedir.HomeDir()
		if home != "" {
			kubeconfigPath = filepath.Join(home, ".kubernetes", "config")
		} else {
			panic("Cannot find kubernetes configuration; cannot resolve home directory.")
		}

		// use the current context in kubeconfig
		kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			panic(err.Error())
		}

		// Creates the Clientset.
		clientset, err := kubernetes.NewForConfig(kubeConfig)
		if err != nil {
			panic(err.Error())
		}

		dynamicConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			panic(err.Error())
		}

		// Create the "Dynamic" client, which is used for unstructured components, such as CloneSets.
		dynamicClient, err := dynamic.NewForConfig(dynamicConfig)
		if err != nil {
			panic(err.Error())
		}

		client.kubeClientset = clientset
		client.dynamicClient = dynamicClient
	} else if clusterDaemonOptions.IsKubernetesMode() {
		kubeConfig, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}

		// Creates the Clientset.
		clientset, err := kubernetes.NewForConfig(kubeConfig)
		if err != nil {
			panic(err.Error())
		}

		dynamicConfig, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}

		// Create the "Dynamic" client, which is used for unstructured components, such as CloneSets.
		dynamicClient, err := dynamic.NewForConfig(dynamicConfig)
		if err != nil {
			panic(err.Error())
		}

		client.kubeClientset = clientset
		client.dynamicClient = dynamicClient
	} else {
		panic(fmt.Sprintf("KubeClient can only be created in LocalMode or KubernetesMode. We are in mode: \"%s\"", clusterDaemonOptions.DeploymentMode))
	}

	// Check if the "/configurationFiles" directory exists.
	// Create it if it doesn't already exist.
	if _, err := os.Stat(client.configDir); os.IsNotExist(err) {
		client.log.Debug("The configuration/connection file directory \"%s\" does not exist. Creating it now.", client.configDir)
		os.Mkdir(client.configDir, os.ModePerm)
	}

	switch clusterDaemonOptions.SchedulingPolicy {
	case "default":
		{
			client.schedulingPolicy = "default"
			client.log.Debug("Using the 'DEFAULT' scheduling policy.")
		}
	case "static":
		{
			client.schedulingPolicy = "static"
			client.log.Debug("Using the 'STATIC' scheduling policy.")
		}
	case "dynamic-v3":
		{
			client.schedulingPolicy = "dynamic-v3"
			client.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	case "dynamic-v4":
		{
			client.schedulingPolicy = "dynamic-v4"
			client.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", clusterDaemonOptions.SchedulingPolicy))
		}
	}

	// TODO(Ben): Make the namespace configurable.
	client.createPodWatcher("default")

	return client
}

// AddSchedulingTaintsToNode adds 'NoExecute' and 'NoSchedule' taints to the specified node to prevent Pods from
// being scheduled onto it, and to evict any existing pods that are already scheduled onto it.
func (c *BasicKubeClient) AddSchedulingTaintsToNode(nodeName string) error {
	var patchData = `{
		"spec": {
			"taints": [
				{
					"effect": "NoExecute",
					"key": "key1",
					"value": "value1"
				},
				{
					"effect": "NoSchedule",
					"key": "key1",
					"value": "value1"
				}
			]
		}
	}`

	_, err := c.kubeClientset.CoreV1().Nodes().Patch(context.Background(), nodeName, types.StrategicMergePatchType, []byte(patchData), metav1.PatchOptions{FieldValidation: "strict"})
	if err != nil {
		c.log.Error("Failed to add 'NoExecute' and 'NoSchedule' taints to Kubernetes node '%s': %v", nodeName, err)
		return err
	}

	c.log.Debug("Successfully added 'NoExecute' and 'NoSchedule' taints to Kubernetes node '%s'", nodeName)

	return nil
}

// RemoveAllTaintsFromNode removes all taints from the specified Kubernetes node.
func (c *BasicKubeClient) RemoveAllTaintsFromNode(nodeName string) error {
	var patchData = `{
		"spec": {
			"taints": null
		}
	}`

	_, err := c.kubeClientset.CoreV1().Nodes().Patch(context.Background(), nodeName, types.StrategicMergePatchType, []byte(patchData), metav1.PatchOptions{FieldValidation: "strict"})
	if err != nil {
		c.log.Error("Failed to remove taints from Kubernetes node '%s': %v", nodeName, err)
		return err
	}

	c.log.Debug("Successfully removed all taints from Kubernetes node '%s'", nodeName)

	return nil
}

// PodCreated is a function to be used as the `AddFunc` handler for a Kubernetes SharedInformer.
func (c *BasicKubeClient) PodCreated(obj interface{}) {
	pod := obj.(*corev1.Pod)
	c.log.Debug("Pod created: %s/%s", pod.Namespace, pod.Name)

	// First, check if the newly-created Pod is a kernel Pod.
	// If it is not a kernel Pod, then we simply return.
	if !strings.HasPrefix(pod.Name, "kernel") {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// The first seven characters are "kernel-", and the last 6 characters are "-<ID>", where <ID> is a 5-character.
	kernelId := pod.Name[7:strings.LastIndex(pod.Name, "-")]
	channels, ok := c.scaleUpChannels.Get(kernelId)

	if !ok || len(channels) == 0 {
		c.log.Debug("No scale-up waiters for kernel %s", kernelId)
		return
	}

	// Notify the first wait group that a Pod has started.
	// We only notify one of the wait groups, as each wait group corresponds to
	// a different scale-up operation and thus requires a unique Pod to have been created.
	// We treat the slice of wait groups as FIFO queue.
	var channel chan string
	channel, channels = channels[0], channels[1:]
	channel <- pod.Name // Notify that the Pod has been created by sending its name over the channel.

	c.scaleUpChannels.Set(kernelId, channels)
}

// GetKubernetesNodes returns a list of the current kubernetes nodes.
func (c *BasicKubeClient) GetKubernetesNodes() ([]corev1.Node, error) {
	st := time.Now()
	nodes, err := c.kubeClientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// Note: the calling classes expect this error to be printed here.
		// If for whatever reason this print is removed, then a print will need to be added where the KubeClient::GetKubernetesNodes method is called.
		c.log.Error("Error while retrieving Kubernetes nodes: %v", err)
		return nil, err
	} else {
		c.log.Debug("Successfully refreshed Kubernetes nodes in %v.", time.Since(st))
	}

	return nodes.Items, nil
}

// GetKubernetesNode returns the node with the given name, or nil of that node cannot be found.
func (c *BasicKubeClient) GetKubernetesNode(nodeName string) (*corev1.Node, error) {
	nodes, err := c.kubeClientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", nodeName),
	})

	if err != nil {
		c.log.Error("Error while retrieving Kubernetes nodes: %v", err)
		return nil, err
	}

	if len(nodes.Items) == 0 {
		c.log.Error("Failed to find Kubernetes node with name \"%s\"", nodeName)
		return nil, ErrNodeNotFound
	} else if len(nodes.Items) > 1 {
		c.log.Warn("Multiple nodes returned for query concerning node with name \"%s\"", nodeName)
	}

	return &nodes.Items[0], nil
}

// PodDeleted is a function to be used as the `DeleteFunc` handler for a Kubernetes SharedInformer.
func (c *BasicKubeClient) PodDeleted(obj interface{}) {
	pod := obj.(*corev1.Pod)
	c.log.Debug("Pod deleted: %s/%s", pod.Namespace, pod.Name)

	// First, check if the newly-created Pod is a kernel Pod.
	// If it is not a kernel Pod, then we simply return.
	if !strings.HasPrefix(pod.Name, "kernel") {
		return
	}

	channel, ok := c.scaleDownChannels.Get(pod.Name)
	if !ok {
		return
	}

	channel <- struct{}{}
}

// PodUpdated is a function to be used as the `UpdateFunc` handler for a Kubernetes SharedInformer.
func (c *BasicKubeClient) PodUpdated(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*corev1.Pod)
	newPod := newObj.(*corev1.Pod)
	if newPod.Status.Phase == corev1.PodFailed {
		c.log.Warn(
			"Pod updated. %s/%s %s",
			oldPod.Namespace, oldPod.Name, newPod.Status.Phase)
	}
}

// func (c *BasicKubeClient) GetMigrationOperationByNewPod(newPodName string) (MigrationOperation, bool) {
// 	return c.migrationManager.GetMigrationOperationByNewPod(newPodName)
// }

// KubeClientset returns the Kubernetes client.
func (c *BasicKubeClient) KubeClientset() *kubernetes.Clientset {
	return c.kubeClientset
}

// Check if the given Migration Operation has finished. This is called twice: when the new replica registers with the Gateway,
// and when the old Pod is deleted. Whichever of those two events happens last will be the one that designates the operation has having completed.
// func (c *BasicKubeClient) CheckIfMigrationCompleted(op MigrationOperation) bool {
// 	return c.migrationManager.CheckIfMigrationCompleted(op)
// }

// ClusterGateway returns the associated Gateway daemon.
func (c *BasicKubeClient) ClusterGateway() scheduling.ClusterGateway {
	return c.gatewayDaemon
}

// DeleteCloneset deletes the Cloneset for the kernel identified by the given ID.
func (c *BasicKubeClient) DeleteCloneset(kernelId string) error {
	clonesetId := fmt.Sprintf("kernel-%s", kernelId)
	c.log.Debug("Deleting Cloneset '%s' now.", clonesetId)
	// Issue the Kubernetes API request to delete the CloneSet.
	err := c.dynamicClient.Resource(clonesetRes).Namespace("default").Delete(context.TODO(), clonesetId, metav1.DeleteOptions{})

	if err != nil {
		c.log.Error("Error encountered while deleting cloneset '%s': %v", clonesetId, err)
		return err
	}

	return nil
}

// DeployDistributedKernels creates a new Kubernetes StatefulSet for the given Session.
// Returns a tuple containing the connection info returned by the `prepareConnectionFileContents` function and an error,
// which will be nil if there were no errors encountered while creating the StatefulSet and related components.
func (c *BasicKubeClient) DeployDistributedKernels(ctx context.Context, kernel *proto.KernelSpec) (*jupyter.ConnectionInfo, error) {
	c.log.Debug("Creating Kubernetes resources for Kernel %s [Session: %s].", kernel.Id, kernel.Session)

	// Prepare the *jupyter.ConnectionInfo.
	connectionInfo, err := c.prepareConnectionFileContents(kernel)
	if err != nil {
		c.log.Error("Error while preparing connection file: %v.\n", err)
		return nil, err
	}
	c.log.Debug("Prepared connection info: %v\n", connectionInfo)
	for i := 0; i < len(kernel.Argv); i++ {
		c.log.Debug("spec.Kernel.Argv[%d]: %v", i, kernel.Argv[i])
	}

	headlessServiceName := fmt.Sprintf("kernel-%s-svc", kernel.Id)

	// Prepare the *jupyter.ConfigFile.
	configFileInfo, err := c.prepareConfigFileContents(&proto.KernelReplicaSpec{
		ReplicaId: DummySMRNodeId, // We'll replace the dummy value with the correct ID when the Pod starts.
		Replicas:  nil,
		Kernel:    kernel,
	}, headlessServiceName)
	if err != nil {
		c.log.Error("Error while preparing config file: %v.\n", err)
		return nil, err
	}

	// Convert to JSON so we can embed it in a ConfigMap.
	connectionInfoJson, err := json.Marshal(connectionInfo.ToConnectionInfoForKernel())
	if err != nil {
		return nil, err
	}

	// Convert to JSON so we can embed it in a ConfigMap.
	configJson, err := json.Marshal(configFileInfo)
	if err != nil {
		return nil, err
	}

	err = c.createConfigMap(ctx, connectionInfoJson, configJson, kernel)
	if err != nil {
		return nil, err
	}
	c.createHeadlessService(ctx, kernel, connectionInfo, headlessServiceName)

	if c.useStatefulSet {
		panic("This is not supported anymore (out of date implementation).")
		// c.log.Debug("Creating StatefulSet for replicas of kernel \"%s\" now.", kernel.Id)
		// c.log.Warn("Using StatefulSets for deploying kernels is deprecated and is unlikely to work going forward...")
		// err = c.createKernelStatefulSet(ctx, kernel, connectionInfo, headlessServiceName)
	} else {
		c.log.Debug("Creating CloneSet for replicas of kernel \"%s\" now.", kernel.Id)
		err = c.createKernelCloneSet(ctx, kernel, connectionInfo, headlessServiceName)
	}

	if err != nil {
		c.log.Error("Failed to create Kubernetes Resource for kernel %s because: %v", kernel.Id, err)
		return nil, err
	}

	// c.migrationManager.RegisterKernel(kernel.Id)

	return connectionInfo, nil
}

// Return the migration operation associated with the given Kernel ID and SMR Node ID of the new replica.
// func (c *BasicKubeClient) GetMigrationOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (MigrationOperation, bool) {
// 	return c.migrationManager.GetMigrationOperationByKernelIdAndNewReplicaId(kernelId, smrNodeId)
// }

// // TODO(Ben): Will need some sort of concurrency control -- like if we try to migrate two replicas at once, then we'd need to account for this.
// func (c *BasicKubeClient) InitiateKernelMigration(ctx context.Context, targetClient *client.distributedKernelClientImpl, targetSmrNodeId int32, newSpec *gateway.KernelReplicaSpec) (string, error) {
// 	return c.migrationManager.InitiateKernelMigration(ctx, targetClient, targetSmrNodeId, newSpec)
// }

// Add the "apps.kruise.io/specified-delete: true" label to the Pod with the given name.
// The labeled Pod will be prioritized for deletion when the CloneSet is scaled down.
// This is used as part of the replica migration protocol.
//
// Relevant OpenKruise documentation:
// https://openkruise.io/docs/user-manuals/cloneset/#selective-pod-deletion
//
// Currently unusued. We can modify the CloneSet directly.
// func (c *BasicKubeClient) addKruiseDeleteLabelToPod(podName string, podNamespace string) error {
// 	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
// 		payload := `{"metadata": {"labels": {"apps.kruise.io/specified-delete": "true"}}}`
// 		_, updateErr := c.kubeClientset.CoreV1().Pods(podNamespace).Patch(context.Background(), podName, types.MergePatchType, []byte(payload), metav1.PatchOptions{})
// 		if updateErr != nil {
// 			c.log.Error("Error when updating labels for Pod \"%s\": %v", podName, updateErr)
// 			return errors.Wrapf(updateErr, fmt.Sprintf("Failed to add deletion label to Pod \"%s\".", podName))
// 		}

// 		c.log.Debug("Pod %s labelled successfully.", podName)
// 		return nil
// 	})

// 	if retryErr != nil {
// 		c.log.Error("Failed to update metadata labels for old Pod %s/%s", podNamespace, podName)
// 		return retryErr
// 	}

// 	return nil
// }

// RegisterChannel registers a channel that is used to notify waiting goroutines that the Pod/Container has started.
func (c *BasicKubeClient) RegisterChannel(kernelId string, startedChan chan string) {
	// Store the new channel in the mapping.
	channels, ok := c.scaleUpChannels.Get(kernelId)
	if !ok {
		channels = make([]chan string, 0, 4)
	}
	channels = append(channels, startedChan)
	c.scaleUpChannels.Set(kernelId, channels)
}

// ScaleOutCloneSet scales-up a CloneSet by increasing its number of replicas by 1.
//
// Accepts as a parameter a chan string that can be used to wait until the new Pod has been created.
// The name of the new Pod will be sent over the channel when the new Pod is started.
// The error will be nil on success.
//
// Parameters:
// - kernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale-out.
// - podStartedChannel (chan string): Used to notify waiting goroutines that the Pod has started.
func (c *BasicKubeClient) ScaleOutCloneSet(kernelId string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// The CloneSet resources for distributed kernels are named "kernel-<kernel ID>".
	clonesetId := fmt.Sprintf("kernel-%s", kernelId)

	// This is the same as retry.DefaultRetry according to:
	// https://pkg.go.dev/k8s.io/client-go/util/retry#pkg-variables
	//
	// Including it here explicitly for clarity.
	var retryParameters = wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Millisecond,
		Factor:   1.0,
		Jitter:   0.1,
	}

	// Increase the number of replicas.
	retryErr := retry.RetryOnConflict(retryParameters, func() error {
		result, getErr := c.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Get(context.TODO(), clonesetId, metav1.GetOptions{})

		if getErr != nil {
			panic(fmt.Errorf("failed to get latest version of CloneSet \"%s\": %v", clonesetId, getErr))
		}

		currentNumReplicas, found, err := unstructured.NestedInt64(result.Object, "spec", "replicas")

		if err != nil || !found {
			c.log.Error("Replicas not found for CloneSet %s: error=%s", clonesetId, err)
			return err
		}

		c.log.Debug("Attempting to INCREASE the number of replicas of CloneSet \"%s\". Currently, it is configured to have %d replicas.", clonesetId, currentNumReplicas)
		newNumReplicas := currentNumReplicas + 1

		// Increase the number of replicas.
		if err := unstructured.SetNestedField(result.Object, newNumReplicas, "spec", "replicas"); err != nil {
			panic(fmt.Errorf("failed to set replica value for CloneSet \"%s\": %v", clonesetId, err))
		}

		_, updateErr := c.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Update(context.TODO(), result, metav1.UpdateOptions{})

		if updateErr != nil {
			c.log.Error("Failed to apply update to CloneSet \"%s\": error=%s", clonesetId, err)
		} else {
			c.log.Debug("Successfully increased number of replicas of CloneSet \"%s\" to %d.", clonesetId, newNumReplicas)
		}

		return updateErr
	})

	if retryErr != nil {
		// Store the new channel in the mapping.
		channels, ok := c.scaleUpChannels.Get(kernelId)
		if !ok {
			panic(fmt.Sprintf("Expected to find slice of scale-up channels for kernel %s.", kernelId))
		}
		// Remove the channel that we just added, as the scale-out operation failed.
		channels = channels[:len(channels)-1]
		c.scaleUpChannels.Set(kernelId, channels)

		return errors.Wrapf(retryErr, "Error when attempting to scale-up CloneSet %s", clonesetId)
	}

	return nil
}

// ScaleInCloneSet scales-down a CloneSet by decreasing its number of replicas by 1.
//
// Parameters:
// - kernelId (string): The ID of the kernel associated with the CloneSet that we'd like to scale in
// - oldPodName (string): The name of the Pod that we'd like to delete during the scale-in operation.
// - podStoppedChannel (chan struct{}): Used to notify waiting goroutines that the Pod has stopped.
func (c *BasicKubeClient) ScaleInCloneSet(kernelId string, oldPodName string, podStoppedChannel chan struct{}) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	clonesetId := fmt.Sprintf("kernel-%s", kernelId)
	c.log.Debug("Scaling-in CloneSet %s by deleting Pod %s.", clonesetId, oldPodName)
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, getErr := c.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Get(context.TODO(), clonesetId, metav1.GetOptions{})

		if getErr != nil {
			panic(fmt.Errorf("failed to get latest version of CloneSet \"%s\": %v", clonesetId, getErr))
		}

		currentNumReplicas, found, err := unstructured.NestedInt64(result.Object, "spec", "replicas")

		if err != nil || !found {
			c.log.Error("Replicas not found for CloneSet %s: error=%v", clonesetId, err)
			return err
		}

		// COMMENTED-OUT:
		// We can modify the CloneSet directly.
		//
		// Label the Pod that we would like to delete so that the CloneSet prioritizes deleting it when we scale it down in the next step.
		// err = c.addKruiseDeleteLabelToPod(oldPodName, "default")
		// if err != nil {
		// 	panic(err)
		// }

		c.log.Debug("Attempting to DECREASE the number of replicas of CloneSet \"%s\" by deleting pod \"%s\". Currently, it is configured to have %d replicas.", clonesetId, oldPodName, currentNumReplicas)
		newNumReplicas := currentNumReplicas - 1

		// Decrease the number of replicas.
		if err := unstructured.SetNestedField(result.Object, newNumReplicas, "spec", "replicas"); err != nil {
			panic(fmt.Errorf("failed to set spec.replicas value for CloneSet \"%s\": %v", clonesetId, err))
		}

		if err := unstructured.SetNestedField(result.Object, []interface{}{oldPodName}, "spec", "scaleStrategy", "podsToDelete"); err != nil {
			panic(fmt.Errorf("failed to set spec.scaleStrategy.podsToDelete value for CloneSet \"%s\": %v", clonesetId, err))
		}

		_, updateErr := c.dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Update(context.TODO(), result, metav1.UpdateOptions{})

		if updateErr != nil {
			c.log.Error("Failed to apply update to CloneSet \"%s\": error=%v", clonesetId, updateErr)
		} else {
			c.log.Debug("Successfully decreased number of replicas of CloneSet \"%s\" to %d.", clonesetId, newNumReplicas)
		}

		return updateErr // Will be nil if the operation was successful.
	})

	c.scaleDownChannels.Set(oldPodName, podStoppedChannel)

	if retryErr != nil {
		c.log.Error("Failed to scale-in CloneSet %s: %v", clonesetId, retryErr)
	}

	// Store the channel in the mapping.
	// channels, ok := c.scaleDownChannels.Get(kernelId)
	// if !ok {
	// 	channels = make([]chan string, 0, 4)
	// }
	// channels = append(channels, podStoppedChannel)
	// c.scaleDownChannels.Set(kernelId, channels)

	return retryErr
}

// createPodWatcher creates a SharedInformer that watches for Pod-creation and Pod-deletion events within the given namespace.
// In general, namespace should be "default" until we make the namespace configurable (for the Helm k8s deployment).
// This is expected to be used in conjunction with the Migration Orchestrator, as the Migration Orchestrator exposes
// an API that is registered with the SharedInformer to handle Pod-started and Pod-stopped events.
func (c *BasicKubeClient) createPodWatcher(namespace string) {
	// create shared informers for resources in all known API group versions with a reSync period and namespace
	factory := informers.NewSharedInformerFactoryWithOptions(c.kubeClientset, 15*time.Second, informers.WithNamespace(namespace))
	podInformer := factory.Core().V1().Pods().Informer()
	go factory.Start(c.podWatcherStopChan)

	// start to sync and call list
	if !cache.WaitForCacheSync(c.podWatcherStopChan, podInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// Temporary.
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.PodCreated,
		UpdateFunc: c.PodUpdated,
		DeleteFunc: c.PodDeleted,
	})
}

// createKernelStatefulSet creates a StatefulSet for a particular distributed kernel.
//
// Parameters:
// - ctx (context.Context): Context object.
// - kernel (*gateway.KernelSpec): The specification of the distributed kernel.
// - connectionInfo (*jupyter.ConnectionInfo): The connection info of the distributed kernel.
// - headlessServiceName (string): The name of the headless Kubernetes service that was created to manage the networking of the Pods of the StatefulSet.
func (c *BasicKubeClient) createKernelStatefulSet(ctx context.Context, kernel *proto.KernelSpec, connectionInfo *jupyter.ConnectionInfo, headlessServiceName string) error {
	// Create the StatefulSet of distributed kernel replicas.
	statefulSetsClient := c.kubeClientset.AppsV1().StatefulSets(metav1.NamespaceDefault)
	var replicas int32 = 3
	var storageClassName = "local-path"
	var affinity = corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					TopologyKey: "kubernetes.io/hostname",
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
						},
					},
				},
			},
		},
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "schedule-kernels",
								Operator: corev1.NodeSelectorOpIn,
								Values: []string{
									kernel.Id,
								},
							},
						},
					},
				},
			},
		},
	}

	storageResource, err := resource.ParseQuantity("128Mi")
	if err != nil {
		panic(err)
	}
	var defaultMode int32 = 0777
	statefulSet := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/metav1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
				"app":    fmt.Sprintf("kernel-%s", kernel.Id),
			},
			Name: fmt.Sprintf("kernel-%s", kernel.Id),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Ordinals: &appsv1.StatefulSetOrdinals{
				Start: 1, // We want to start at 1, as we also use the ordinals as the SMR Node IDs, and those are expected to begin at 1.
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{}},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": fmt.Sprintf("kernel-%s", kernel.Id),
				},
			},
			ServiceName: headlessServiceName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
						"app":    fmt.Sprintf("kernel-%s", kernel.Id),
					},
				},
				Spec: corev1.PodSpec{
					Affinity:      &affinity,
					RestartPolicy: corev1.RestartPolicyAlways,
					Volumes: []corev1.Volume{
						{
							Name: "kernel-configmap",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: fmt.Sprintf("kernel-%s-configmap", strings.ToLower(kernel.Id)),
									},
									DefaultMode: &defaultMode,
								},
							},
						},
						{
							Name: "kernel-entrypoint",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "kernel-entrypoint-configmap",
									},
									DefaultMode: &defaultMode,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "kernel",
							Image: fmt.Sprintf("%s:%s", c.notebookImageName, c.notebookImageTag), // TODO(Ben): Don't hardcode this.
							Command: []string{
								"/kernel-entrypoint/kernel-entrypoint.sh",
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8888,
								},
								{
									ContainerPort: int32(connectionInfo.ControlPort),
								},
								{
									ContainerPort: int32(connectionInfo.HBPort),
								},
								{
									ContainerPort: int32(connectionInfo.IOPubPort),
								},
								{
									ContainerPort: int32(connectionInfo.IOSubPort),
								},
								{
									ContainerPort: int32(connectionInfo.ShellPort),
								},
								{
									ContainerPort: int32(connectionInfo.StdinPort),
								},
								{
									ContainerPort: int32(connectionInfo.AckPort),
								},
								{
									ContainerPort: int32(c.smrPort),
								},
								{
									ContainerPort: 8464,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "node-local",
									MountPath: c.nodeLocalMountPoint,
								},
								{
									Name:      "kernel-configmap",
									MountPath: c.configDir,
									ReadOnly:  false,
								},
								{
									Name:      "kernel-entrypoint",
									MountPath: "/kernel-entrypoint",
									ReadOnly:  false,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "POD_SERVICE_ACCOUNT",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.serviceAccountName",
										},
									},
								},
								{
									Name: "NODE_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.hostIP",
										},
									},
								},
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "CONNECTION_FILE_PATH",
									Value: fmt.Sprintf("%s/connection-file.json", c.configDir),
								},
								{
									Name:  IPythonConfigPath,
									Value: c.ipythonConfigPath,
								},
								{
									Name:  "SESSION_ID",
									Value: kernel.Session,
								},
								{
									Name:  "KERNEL_ID",
									Value: kernel.Id,
								},
								{
									Name:  "LOCAL_DAEMON_SERVICE_NAME",
									Value: c.localDaemonServiceName,
								},
								{
									Name:  "LOCAL_DAEMON_SERVICE_PORT",
									Value: fmt.Sprintf("%d", c.localDaemonServicePort),
								},
								{
									Name:  "KERNEL_NETWORK_SERVICE_NAME",
									Value: headlessServiceName,
								},
								{
									Name:  "DEPLOYMENT_MODE",
									Value: "kubernetes",
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-local",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: &storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: storageResource,
							},
						},
					},
				},
			},
		},
	}

	_, err = statefulSetsClient.Create(ctx, statefulSet, metav1.CreateOptions{})

	if err != nil {
		c.log.Error("Failed to create StatefulSet for kernel %s because: %v", kernel.Id, err)
	}

	return err
}

func (c *BasicKubeClient) getSchedulerName() string {
	return fmt.Sprintf("%s-scheduler", c.schedulingPolicy)
}

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// Create a CloneSet for a particular distributed kernel.
//
// Parameters:
// - ctx (context.Context): Context object.
// - kernel (*gateway.KernelSpec): The specification of the distributed kernel.
// - connectionInfo (*jupyter.ConnectionInfo): The connection info of the distributed kernel.
// - headlessServiceName (string): The name of the headless Kubernetes service that was created to manage the networking of the Pods of the CloneSet.
func (c *BasicKubeClient) createKernelCloneSet(ctx context.Context, kernel *proto.KernelSpec, connectionInfo *jupyter.ConnectionInfo, headlessServiceName string) error {
	var kernelResourceRequirements = kernel.GetResourceSpec()
	if kernelResourceRequirements == nil {
		kernelResourceRequirements = &proto.ResourceSpec{
			Gpu:    0,
			Cpu:    0,
			Memory: 0,
		}
	}

	nodes, err := c.kubeClientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		c.log.Error("Failed to list Kubernetes nodes because: %v", err)
		panic(err)
	}

	// Remove the control-plane node.
	for _, node := range nodes.Items {
		if strings.HasSuffix(node.Name, "control-plane") {
			continue
		}

		c.log.Debug("Updating node %s to have label `%s=\"true\"`.", node.Name, kernel.Id)
		payload := []patchStringValue{{
			Op:    "replace",
			Path:  fmt.Sprintf("/metadata/labels/%s", kernel.Id),
			Value: "true",
		}}
		payloadBytes, _ := json.Marshal(payload)
		updatedNode, err := c.kubeClientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
		if err != nil {
			c.log.Error("Failed to add label `%s=\"true\"` to node %s. Reason: %v.", kernel.Id, node.Name, err)
			return err
		} else {
			c.log.Debug("Successfully added label `%s=\"true\" to node %s.", kernel.Id, updatedNode.Name)
		}
	}

	// If we're using the "default" scheduling policy, then they will just have podAntiAffinity.
	// If we're using static or dynamic scheduling, then the nodes will have a nodeAffinity in addition to the podAntiAffinity.
	var affinity map[string]interface{}
	if c.schedulingPolicy == "static" || c.schedulingPolicy == "dynamic-v3" || c.schedulingPolicy == "dynamic-v4" {
		affinity = map[string]interface{}{
			"podAntiAffinity": map[string]interface{}{
				"requiredDuringSchedulingIgnoredDuringExecution": []map[string]interface{}{
					{
						"topologyKey": "kubernetes.io/hostname",
						"labelSelector": map[string]interface{}{
							"matchLabels": map[string]interface{}{
								"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
							},
						},
					},
				},
			},
			"nodeAffinity": map[string]interface{}{
				"requiredDuringSchedulingIgnoredDuringExecution": map[string]interface{}{
					"nodeSelectorTerms": []map[string]interface{}{
						{
							"matchExpressions": []map[string]interface{}{
								{
									"key":      kernel.GetId(),
									"operator": "In",
									"values": []string{
										"true",
									},
								},
							},
						},
					},
				},
			},
		}
	} else {
		affinity = map[string]interface{}{
			"podAntiAffinity": map[string]interface{}{
				"requiredDuringSchedulingIgnoredDuringExecution": []map[string]interface{}{
					{
						"topologyKey": "kubernetes.io/hostname",
						"labelSelector": map[string]interface{}{
							"matchLabels": map[string]interface{}{
								"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
							},
						},
					},
				},
			},
		}
	}

	// Define the CloneSet.
	cloneSetDefinition := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps.kruise.io/v1alpha1",
			"kind":       "CloneSet",
			"metadata": map[string]interface{}{
				"name": fmt.Sprintf("kernel-%s", kernel.Id),
				"labels": map[string]interface{}{
					"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
					"app":    fmt.Sprintf("kernel-%s", kernel.Id),
				},
			},
			"spec": map[string]interface{}{
				"replicas": 3,
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app":    fmt.Sprintf("kernel-%s", kernel.Id),
						"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
					},
				},
				// "serviceName": headlessServiceName,
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": fmt.Sprintf("kernel-%s", kernel.Id),
						"labels": map[string]interface{}{
							"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
							"app":    fmt.Sprintf("kernel-%s", kernel.Id),
						},
					},
					"spec": map[string]interface{}{
						"affinity": affinity,
						"volumes": []map[string]interface{}{
							{
								"name": "kernel-configmap",
								"configMap": map[string]interface{}{
									"name":        fmt.Sprintf("kernel-%s-configmap", strings.ToLower(kernel.Id)),
									"defaultMode": int32(0777),
								},
							},
							{
								"name": "kernel-entrypoint",
								"configMap": map[string]interface{}{
									"name":        "kernel-entrypoint-configmap",
									"defaultMode": int32(0777),
								},
							},
							{
								"name": "scheduling-dump-mount",
								"hostPath": map[string]interface{}{
									"path": "/home/scheduling-dump",
								},
							},
						},
						"schedulerName": c.getSchedulerName(),
						"containers": []map[string]interface{}{
							{
								"name":    "kernel",
								"image":   fmt.Sprintf("%s:%s", c.notebookImageName, c.notebookImageTag),
								"command": []string{"/kernel-entrypoint/kernel-entrypoint.sh"},
								"resources": map[string]interface{}{
									"limits": map[string]interface{}{
										"memory":                         fmt.Sprintf("%fMi", kernelResourceRequirements.Memory),
										"cpu":                            fmt.Sprintf("%dm", kernelResourceRequirements.Cpu),
										"ds2-lab.github.io/deflated-gpu": fmt.Sprintf("%d", kernelResourceRequirements.Gpu),
									},
									"requests": map[string]interface{}{
										"memory":                         fmt.Sprintf("%fMi", kernelResourceRequirements.Memory),
										"cpu":                            fmt.Sprintf("%dm", kernelResourceRequirements.Cpu),
										"ds2-lab.github.io/deflated-gpu": fmt.Sprintf("%d", kernelResourceRequirements.Gpu),
									},
								},
								"ports": []map[string]interface{}{
									{
										"containerPort": 8888,
									},
									{
										"containerPort": int32(connectionInfo.ControlPort),
									},
									{
										"containerPort": int32(connectionInfo.HBPort),
									},
									{
										"containerPort": int32(connectionInfo.IOPubPort),
									},
									{
										"containerPort": int32(connectionInfo.IOSubPort),
									},
									{
										"containerPort": int32(connectionInfo.ShellPort),
									},
									{
										"containerPort": int32(connectionInfo.StdinPort),
									},
									{
										"containerPort": int32(connectionInfo.AckPort),
									},
									{
										"containerPort": int32(c.smrPort),
									},
									{
										"containerPort": 8464,
									},
								},
								"volumeMounts": []map[string]interface{}{
									{
										"name":      "kernel-configmap",
										"mountPath": c.configDir,
									},
									{
										"name":      "kernel-entrypoint",
										"mountPath": "/kernel-entrypoint",
									},
									{
										"name":      "scheduling-dump-mount",
										"mountPath": "/tmp/cores",
									},
								},
								"env": []map[string]interface{}{
									{
										"name": "POD_SERVICE_ACCOUNT",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "spec.serviceAccountName",
											},
										},
									},
									{
										"name": "NODE_IP",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "status.hostIP",
											},
										},
									},
									{
										"name": "POD_IP",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "status.podIP",
											},
										},
									},
									{
										"name": "POD_NAMESPACE",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "metadata.namespace",
											},
										},
									},
									{
										"name": "POD_NAME",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "metadata.name",
											},
										},
									},
									{
										"name": "NODE_NAME",
										"valueFrom": map[string]interface{}{
											"fieldRef": map[string]interface{}{
												"fieldPath": "spec.nodeName",
											},
										},
									},
									{
										"name":  "CONNECTION_FILE_PATH",
										"value": fmt.Sprintf("%s/connection-file.json", c.configDir),
									},
									{
										"name":  "SPEC_CPU",
										"value": fmt.Sprintf("%d", kernelResourceRequirements.Cpu),
									},
									{
										"name":  "SPEC_MEM",
										"value": fmt.Sprintf("%f", kernelResourceRequirements.Memory),
									},
									{
										"name":  "SPEC_GPU",
										"value": fmt.Sprintf("%d", kernelResourceRequirements.Gpu),
									},
									{
										"name":  "SIMULATE_CHECKPOINTING_LATENCY",
										"value": fmt.Sprintf("%v", c.checkpointingEnabled),
									},
									{
										"name":  IPythonConfigPath,
										"value": c.ipythonConfigPath,
									},
									{
										"name":  "SESSION_ID",
										"value": kernel.Session,
									},
									{
										"name":  "KERNEL_ID",
										"value": kernel.Id,
									},
									{
										"name":  "LOCAL_DAEMON_SERVICE_NAME",
										"value": c.localDaemonServiceName,
									},
									{
										"name":  "LOCAL_DAEMON_SERVICE_PORT",
										"value": fmt.Sprintf("%d", c.localDaemonServicePort),
									},
									{
										"name":  "KERNEL_NETWORK_SERVICE_NAME",
										"value": headlessServiceName,
									},
									{
										"name":  "DEPLOYMENT_MODE",
										"value": "kubernetes",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Issue the Kubernetes API request to create the CloneSet.
	_, err = c.dynamicClient.Resource(clonesetRes).Namespace("default").Create(ctx, cloneSetDefinition, metav1.CreateOptions{})

	return err
}

// Create a Kubernetes ConfigMap containing the configuration information for a particular deployment of distributed kernels.
// Both the connectionInfoJson and configJson arguments should be values returned by the json.Marshal function.
func (c *BasicKubeClient) createConfigMap(ctx context.Context, connectionInfoJson []byte, configJson []byte, kernel *proto.KernelSpec) error {
	// Construct the ConfigMap. We'll mount this to the Pods.
	connectionFileConfigMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "metav1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("kernel-%s-configmap", strings.ToLower(kernel.Id)),
			Namespace: "default", // TODO(Ben): Don't hardcode the namespace.
		},
		Data: map[string]string{
			"connection-file.json": string(connectionInfoJson),
			"ipython_config.json":  string(configJson),
		},
	}

	// Create the ConfigMap using the Kubernetes API.
	// TODO(Ben): Don't hardcode the namespace.
	_, err := c.kubeClientset.CoreV1().ConfigMaps("default").Create(ctx, connectionFileConfigMap, metav1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating ConfigMap for connection file for Session %s: %v", kernel.Id, err)
	}

	return err // Will be nil if no error occurred.
}

// Create a headless service that will control the networking of the distributed kernel StatefulSet.
func (c *BasicKubeClient) createHeadlessService(ctx context.Context, kernel *proto.KernelSpec, connectionInfo *jupyter.ConnectionInfo, serviceName string) {
	// Create a headless service for the StatefulSet that we'll be creating later on.
	svcClient := c.kubeClientset.CoreV1().Services(corev1.NamespaceDefault)
	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "web",
					Protocol: "TCP",
					Port:     80,
					TargetPort: intstr.IntOrString{
						IntVal: 80,
					},
				},
				{
					Name:     "control-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.ControlPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.ControlPort),
					},
				},
				{
					Name:     "shell-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.ShellPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.ShellPort),
					},
				},
				{
					Name:     "stdin-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.StdinPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.StdinPort),
					},
				},
				{
					Name:     "hb-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.HBPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.HBPort),
					},
				},
				{
					Name:     "iopub-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.IOPubPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.IOPubPort),
					},
				},
				{
					Name:     "iosub-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.IOSubPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.IOSubPort),
					},
				},
				{
					Name:     "smr-port",
					Protocol: "TCP",
					Port:     int32(c.smrPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(c.smrPort),
					},
				},
				{
					Name:     "ack-port",
					Protocol: "TCP",
					Port:     int32(connectionInfo.AckPort),
					TargetPort: intstr.IntOrString{
						IntVal: int32(connectionInfo.AckPort),
					},
				},
				{
					Name:     "debug-port",
					Protocol: "TCP",
					Port:     8464,
					TargetPort: intstr.IntOrString{
						IntVal: 8464,
					},
				},
			},
			Selector:  map[string]string{"app": fmt.Sprintf("kernel-%s", kernel.Id)},
			ClusterIP: "None", // Headless.
			Type:      corev1.ServiceTypeClusterIP,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   serviceName,
			Labels: map[string]string{"app": fmt.Sprintf("kernel-%s", kernel.Id)},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "metav1",
		},
	}
	_, err := svcClient.Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating Service for StatefulSet for Session %s.", kernel.Id)
		panic(err)
	}
}

func (c *BasicKubeClient) prepareConnectionFileContents(spec *proto.KernelSpec) (*jupyter.ConnectionInfo, error) {
	// Prepare contents of the connection file.
	// We just need to add the SignatureScheme and Key.
	// The other information will be available in a file already on the host.
	connectionInfo := &jupyter.ConnectionInfo{
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
		ControlPort:     c.gatewayDaemon.ConnectionOptions().ControlPort,
		ShellPort:       c.gatewayDaemon.ConnectionOptions().ShellPort,
		StdinPort:       c.gatewayDaemon.ConnectionOptions().StdinPort,
		HBPort:          c.gatewayDaemon.ConnectionOptions().HBPort,
		IOPubPort:       c.gatewayDaemon.ConnectionOptions().IOPubPort,
		IOSubPort:       c.gatewayDaemon.ConnectionOptions().IOSubPort,
		AckPort:         c.gatewayDaemon.ConnectionOptions().AckPort,
		Transport:       "tcp",
		IP:              "0.0.0.0",
	}

	return connectionInfo, nil
}

func (c *BasicKubeClient) prepareConfigFileContents(spec *proto.KernelReplicaSpec, headlessServiceName string) (*jupyter.ConfigFile, error) {
	var replicas []string

	// We can only deterministically construct the hostnames of the replicas if we're using a StatefulSet.
	// This cannot be done with a CloneSet (as far as I am aware).
	if c.useStatefulSet {
		// Fully-qualified domain name.
		fqdnFormat := fmt.Sprintf("kernel-%%s-%%d.%s.%s.svc.cluster.local:%%d", headlessServiceName, c.kubeNamespace)

		// Generate the hostnames for the Pods of the StatefulSet.
		// We can determine them deterministically due to the convention/properties of the StatefulSet.
		for i := 0; i < 3; i++ {
			// We use i+1 here, as SMR IDs are expected to begin at 1, and we configured the StatefulSet of kernel replicas to begin ordinals at 1 rather than 0.
			fqdn := fmt.Sprintf(fqdnFormat, spec.ID(), i+1, c.smrPort)
			c.log.Debug("Generated peer fully-qualified domain name: \"%s\"", fqdn)
			replicas = append(replicas, fqdn)
		}
	} else {
		replicas = append(replicas, "")
		replicas = append(replicas, "")
		replicas = append(replicas, "")
	}

	// Prepare contents of the configuration file.
	file := &jupyter.ConfigFile{
		DistributedKernelConfig: jupyter.DistributedKernelConfig{
			StorageBase:             kubeStorageBase,
			SMRNodeID:               -1, // int(spec.ReplicaId), // TODO(Ben): Set this to -1 to make it obvious that the Pod needs to fill this in itself?
			SMRNodes:                replicas,
			SMRJoin:                 spec.Join,
			SMRPort:                 c.smrPort,
			HdfsNameNodeEndpoint:    c.hdfsNameNodeEndpoint,
			RegisterWithLocalDaemon: true,
			LocalDaemonAddr:         "", // This is only used in Docker mode.
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}
