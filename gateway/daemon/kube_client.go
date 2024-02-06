package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
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
)

type BasicKubeClient struct {
	kubeClientset          *kubernetes.Clientset  // Clientset contains the clients for groups. Each group has exactly one version included in a Clientset.
	dynamicClient          *dynamic.DynamicClient // Dynamic client for working with unstructured components. We use this for the custom CloneSet.
	gatewayDaemon          *GatewayDaemon         // Associated Gateway daemon.
	configDir              string                 // Where to write config files. This is also where they'll be found on the kernel nodes.
	ipythonConfigPath      string                 // Where the IPython config is located.
	nodeLocalMountPoint    string                 // The mount of the shared PVC for all kernel nodes.
	localDaemonServiceName string                 // Name of the service controlling the routing of the local daemon. It only routes traffic on the same node.
	localDaemonServicePort int                    // Port that local daemon service will be routing traffic to.
	smrPort                int                    // Port used for the SMR protocol.
	kubeNamespace          string                 // Kubernetes namespace that all of these components reside in.
	useStatefulSet         bool                   // If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet.
	podWatcherStopChan     chan struct{}          // Used to tell the Pod Watcher to stop.
	migrationManager       MigrationManager       // Responsible for orchestrating and managing migration operations.
	log                    logger.Logger
}

func NewKubeClient(gatewayDaemon *GatewayDaemon, daemonKubeClientOptions *DaemonKubeClientOptions) *BasicKubeClient {
	client := &BasicKubeClient{
		configDir:              utils.GetEnv(KubeSharedConfigDir, KubeSharedConfigDirDefault),
		ipythonConfigPath:      utils.GetEnv(IPythonConfigPath, IPythonConfigPathDefault),
		nodeLocalMountPoint:    utils.GetEnv(KubeNodeLocalMountPoint, KubeNodeLocalMountPointDefault),
		localDaemonServiceName: daemonKubeClientOptions.LocalDaemonServiceName,
		localDaemonServicePort: daemonKubeClientOptions.LocalDaemonServicePort,
		smrPort:                daemonKubeClientOptions.SMRPort,
		kubeNamespace:          daemonKubeClientOptions.KubeNamespace,
		gatewayDaemon:          gatewayDaemon,
		useStatefulSet:         daemonKubeClientOptions.UseStatefulSet,
		podWatcherStopChan:     make(chan struct{}),
	}

	config.InitLogger(&client.log, client)

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

	// Check if the "/configurationFiles" directory exists.
	// Create it if it doesn't already exist.
	if _, err := os.Stat(client.configDir); os.IsNotExist(err) {
		client.log.Debug("The configuration/connection file directory \"%s\" does not exist. Creating it now.", client.configDir)
		os.Mkdir(client.configDir, os.ModePerm)
	}

	// smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	// if smrPort == 0 {
	// 	smrPort = KernelSMRPortDefault
	// }

	// client.smrPort = smrPort

	client.migrationManager = NewMigrationManager()

	client.createPodWatcher("default")

	return client
}

// Wait for us to receive a pod-created notification for the given Pod, which managed to start running
// and register with us before we received the pod-created notification. Once received, return the
// associated migration operation.
func (c *BasicKubeClient) WaitForNewPodNotification(newPodName string) MigrationOperation {
	return c.migrationManager.WaitForNewPodNotification(newPodName)
}

func (c *BasicKubeClient) GetMigrationOperationByNewPod(newPodName string) (MigrationOperation, bool) {
	return c.migrationManager.GetMigrationOperationByNewPod(newPodName)
}

// Get the Kubernetes client.
func (c *BasicKubeClient) KubeClientset() *kubernetes.Clientset {
	return c.kubeClientset
}

// Check if the given Migration Operation has finished. This is called twice: when the new replica registers with the Gateway,
// and when the old Pod is deleted. Whichever of those two events happens last will be the one that designates the operation has having completed.
func (c *BasicKubeClient) CheckIfMigrationCompleted(op MigrationOperation) bool {
	return c.migrationManager.CheckIfMigrationCompleted(op)
}

// Get the associated Gateway daemon.
func (c *BasicKubeClient) GatewayDaemon() *GatewayDaemon {
	return c.gatewayDaemon
}

func (c *BasicKubeClient) GenerateKernelName(sessionId string) string {
	return fmt.Sprintf(KubernetesKernelName, fmt.Sprintf("%s", sessionId))
}

// Create a new Kubernetes StatefulSet for the given Session.
// Returns a tuple containing the connection info returned by the `prepareConnectionFileContents` function and an error,
// which will be nil if there were no errors encountered while creating the StatefulSet and related components.
func (c *BasicKubeClient) DeployDistributedKernels(ctx context.Context, kernel *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	c.log.Debug("Creating StatefulSet for Session %s.", kernel.Id)

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
	configFileInfo, err := c.prepareConfigFileContents(&gateway.KernelReplicaSpec{
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
		panic(err)
	}

	// Convert to JSON so we can embed it in a ConfigMap.
	configJson, err := json.Marshal(configFileInfo)
	if err != nil {
		panic(err)
	}

	c.createConfigMap(ctx, connectionInfoJson, configJson, kernel)
	c.createHeadlessService(ctx, kernel, connectionInfo, headlessServiceName)

	if c.useStatefulSet {
		c.log.Debug("Creating StatefulSet for replicas of kernel \"%s\" now.", kernel.Id)
		c.createKernelStatefulSet(ctx, kernel, connectionInfo, headlessServiceName)
	} else {
		c.log.Debug("Creating CloneSet for replicas of kernel \"%s\" now.", kernel.Id)
		c.createKernelCloneSet(ctx, kernel, connectionInfo, headlessServiceName)
	}

	c.migrationManager.RegisterKernel(kernel.Id)

	return connectionInfo, nil
}

// TODO(Ben): Will need some sort of concurrency control -- like if we try to migrate two replicas at once, then we'd need to account for this.
func (c *BasicKubeClient) InitiateKernelMigration(ctx context.Context, targetClient *client.DistributedKernelClient, targetSmrNodeId int32, persistentId string) error {
	return c.migrationManager.InitiateKernelMigration(ctx, targetClient, targetSmrNodeId, persistentId)
}

// Create a SharedInformer that watches for Pod-creation and Pod-deletion events within the given namespace.
// In general, namespace should be "default" until we make the namespace configurable (for the Helm k8s deployment).
// This is expected to be used in conjunction with the Migration Orchestrator, as the Migration Orchestrator exposes
// an API that is registered with the SharedInformer to handle Pod-started and Pod-stopped events.
func (c *BasicKubeClient) createPodWatcher(namespace string) {
	// create shared informers for resources in all known API group versions with a reSync period and namespace
	factory := informers.NewSharedInformerFactoryWithOptions(c.kubeClientset, 10*time.Second, informers.WithNamespace(namespace))
	podInformer := factory.Core().V1().Pods().Informer()
	go factory.Start(c.podWatcherStopChan)

	// start to sync and call list
	if !cache.WaitForCacheSync(c.podWatcherStopChan, podInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	// Temporary.
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.migrationManager.PodCreated,
		UpdateFunc: c.migrationManager.PodUpdated,
		DeleteFunc: c.migrationManager.PodDeleted,
	})
}

// Create a StatefulSet for a particular distributed kernel.
//
// Parameters:
// - ctx (context.Context): Context object.
// - kernel (*gateway.KernelSpec): The specification of the distributed kernel.
// - connectionInfo (*jupyter.ConnectionInfo): The connection info of the distributed kernel.
// - headlessServiceName (string): The name of the headless Kubernetes service that was created to manage the networking of the Pods of the StatefulSet.
func (c *BasicKubeClient) createKernelStatefulSet(ctx context.Context, kernel *gateway.KernelSpec, connectionInfo *jupyter.ConnectionInfo, headlessServiceName string) {
	// Create the StatefulSet of distributed kernel replicas.
	statefulSetsClient := c.kubeClientset.AppsV1().StatefulSets(v1.NamespaceDefault)
	var replicas int32 = 3
	var storageClassName string = "local-path"
	var affinity corev1.Affinity = corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					TopologyKey: "kubernetes.io/hostname",
					LabelSelector: &v1.LabelSelector{
						MatchLabels: map[string]string{
							"kernel": fmt.Sprintf("kernel-%s", kernel.Id),
						},
					},
				},
			},
		},
	}

	storage_resource, err := resource.ParseQuantity("128Mi")
	if err != nil {
		panic(err)
	}
	var defaultMode int32 = 0777
	statefulSet := &appsv1.StatefulSet{
		TypeMeta: v1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: v1.ObjectMeta{
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
			Selector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"app": fmt.Sprintf("kernel-%s", kernel.Id),
				},
			},
			ServiceName: headlessServiceName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: v1.ObjectMeta{
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
										Name: fmt.Sprintf("kernel-%s-configmap", kernel.Id),
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
							Image: "scusemua/jupyter:latest", // TODO(Ben): Don't hardcode this.
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
									ContainerPort: int32(c.smrPort),
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "node-local",
									MountPath: c.nodeLocalMountPoint,
								},
								{
									Name:      "kernel-configmap",
									MountPath: fmt.Sprintf("%s", c.configDir),
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
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: v1.ObjectMeta{
						Name: "node-local",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: &storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: storage_resource,
							},
						},
					},
				},
			},
		},
	}

	_, err = statefulSetsClient.Create(ctx, statefulSet, v1.CreateOptions{})
}

// Create a CloneSet for a particular distributed kernel.
//
// Parameters:
// - ctx (context.Context): Context object.
// - kernel (*gateway.KernelSpec): The specification of the distributed kernel.
// - connectionInfo (*jupyter.ConnectionInfo): The connection info of the distributed kernel.
// - headlessServiceName (string): The name of the headless Kubernetes service that was created to manage the networking of the Pods of the CloneSet.
func (c *BasicKubeClient) createKernelCloneSet(ctx context.Context, kernel *gateway.KernelSpec, connectionInfo *jupyter.ConnectionInfo, headlessServiceName string) {
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
						"affinity": map[string]interface{}{
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
						},
						"volumes": []map[string]interface{}{
							{
								"name": "kernel-configmap",
								"configMap": map[string]interface{}{
									"name":        fmt.Sprintf("kernel-%s-configmap", kernel.Id),
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
						},
						"containers": []map[string]interface{}{
							{
								"name":    "kernel",
								"image":   "scusemua/jupyter:latest",
								"command": []string{"/kernel-entrypoint/kernel-entrypoint.sh"},
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
										"containerPort": int32(c.smrPort),
									},
								},
								"volumeMounts": []map[string]interface{}{
									{
										"name":      "kernel-configmap",
										"mountPath": fmt.Sprintf("%s", c.configDir),
									},
									{
										"name":      "kernel-entrypoint",
										"mountPath": "/kernel-entrypoint",
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
								},
							},
						},
					},
				},
			},
		},
	}

	// Issue the Kubernetes API request to create the CloneSet.
	_, err := c.dynamicClient.Resource(clonesetRes).Namespace("default").Create(context.TODO(), cloneSetDefinition, v1.CreateOptions{})

	if err != nil {
		panic(err.Error())
	}
}

// Create a Kubernetes ConfigMap containing the configuration information for a particular deployment of distributed kernels.
// Both the connectionInfoJson and configJson arguments should be values returned by the json.Marshal function.
func (c *BasicKubeClient) createConfigMap(ctx context.Context, connectionInfoJson []byte, configJson []byte, kernel *gateway.KernelSpec) {
	// Construct the ConfigMap. We'll mount this to the Pods.
	connectionFileConfigMap := &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      fmt.Sprintf("kernel-%s-configmap", kernel.Id),
			Namespace: "default", // TODO(Ben): Don't hardcode the namespace.
		},
		Data: map[string]string{
			"connection-file.json": string(connectionInfoJson),
			"ipython_config.json":  string(configJson),
		},
	}

	// Create the ConfigMap using the Kubernetes API.
	// TODO(Ben): Don't hardcode the namespace.
	_, err := c.kubeClientset.CoreV1().ConfigMaps("default").Create(ctx, connectionFileConfigMap, v1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating ConfigMap for connection file for Session %s.", kernel.Id)
		panic(err)
	}
}

// Create a headless service that will control the networking of the distributed kernel StatefulSet.
func (c *BasicKubeClient) createHeadlessService(ctx context.Context, kernel *gateway.KernelSpec, connectionInfo *jupyter.ConnectionInfo, serviceName string) {
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
			},
			Selector:  map[string]string{"app": fmt.Sprintf("kernel-%s", kernel.Id)},
			ClusterIP: "None", // Headless.
			Type:      corev1.ServiceTypeClusterIP,
		},
		ObjectMeta: v1.ObjectMeta{
			Name:   serviceName,
			Labels: map[string]string{"app": fmt.Sprintf("kernel-%s", kernel.Id)},
		},
		TypeMeta: v1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
	}
	_, err := svcClient.Create(ctx, svc, v1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating Service for StatefulSet for Session %s.", kernel.Id)
		panic(err)
	}
}

func (c *BasicKubeClient) prepareConnectionFileContents(spec *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	// Prepare contents of the connection file.
	// We just need to add the SignatureScheme and Key.
	// The other information will be available in a file already on the host.
	connectionInfo := &jupyter.ConnectionInfo{
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
		ControlPort:     c.gatewayDaemon.connectionOptions.ControlPort,
		ShellPort:       c.gatewayDaemon.connectionOptions.ShellPort,
		StdinPort:       c.gatewayDaemon.connectionOptions.StdinPort,
		HBPort:          c.gatewayDaemon.connectionOptions.HBPort,
		IOPubPort:       c.gatewayDaemon.connectionOptions.IOPubPort,
		IOSubPort:       c.gatewayDaemon.connectionOptions.IOSubPort,
		Transport:       "tcp",
		IP:              "0.0.0.0",
	}

	return connectionInfo, nil
}

func (c *BasicKubeClient) prepareConfigFileContents(spec *gateway.KernelReplicaSpec, headlessServiceName string) (*jupyter.ConfigFile, error) {
	var replicas []string

	// We can only deterministically construct the hostnames of the replicas if we're using a StatefulSet.
	// This cannot be done with a CloneSet (as far as I am aware).
	if c.useStatefulSet {
		// Fully-qualified domain name.
		fqdn_format := fmt.Sprintf("kernel-%%s-%%d.%s.%s.svc.cluster.local:%%d", headlessServiceName, c.kubeNamespace)

		// Generate the hostnames for the Pods of the StatefulSet.
		// We can determine them deterministically due to the convention/properties of the StatefulSet.
		for i := 0; i < 3; i++ {
			// We use i+1 here, as SMR IDs are expected to begin at 1, and we configured the StatefulSet of kernel replicas to begin ordinals at 1 rather than 0.
			fqdn := fmt.Sprintf(fqdn_format, spec.ID(), i+1, c.smrPort)
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
			StorageBase: kubeStorageBase,
			SMRNodeID:   -1, // int(spec.ReplicaId), // TODO(Ben): Set this to -1 to make it obvious that the Pod needs to fill this in itself?
			SMRNodes:    replicas,
			SMRJoin:     spec.Join,
			SMRPort:     c.smrPort,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}
