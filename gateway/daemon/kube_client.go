package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/configurationFiles/"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080
)

var (
	kubeStorageBase = "/storage" // TODO(Ben): Don't hard-code this. What should this be?
)

type BasicKubeClient struct {
	kubeClientset       *kubernetes.Clientset // Kubernetes client.
	gatewayDaemon       *GatewayDaemon        // Associated Gateway daemon.
	configDir           string                // Where to write config files. This is also where they'll be found on the kernel nodes.
	nodeLocalMountPoint string                // The mount of the shared PVC for all kernel nodes.
	smrPort             int                   // Port used for the SMR protocol.
	log                 logger.Logger
}

func NewKubeClient(gatewayDaemon *GatewayDaemon) *BasicKubeClient {
	client := &BasicKubeClient{
		configDir:           utils.GetEnv(KubeSharedConfigDir, KubeSharedConfigDir),
		nodeLocalMountPoint: utils.GetEnv(KubeNodeLocalMountPoint, KubeNodeLocalMountPointDefault),
		gatewayDaemon:       gatewayDaemon,
	}
	config.InitLogger(&client.log, client)

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	client.kubeClientset = clientset

	// Check if the "/configurationFiles" directory exists.
	// Create it if it doesn't already exist.
	if _, err := os.Stat(client.configDir); os.IsNotExist(err) {
		client.log.Debug("The configuration/connection file directory \"%s\" does not exist. Creating it now.", client.configDir)
		os.Mkdir(client.configDir, os.ModePerm)
	}

	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}

	client.smrPort = smrPort

	return client
}

// Get the Kubernetes client.
func (c *BasicKubeClient) KubeClientset() *kubernetes.Clientset {
	return c.kubeClientset
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
func (c *BasicKubeClient) CreateKernelStatefulSet(ctx context.Context, kernel *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	c.log.Debug("Creating StatefulSet for Session %s.", kernel.Id)

	// var kernelName string = c.GenerateKernelName(kernel.Id)

	connectionInfo, err := c.prepareConnectionFileContents(kernel)
	if err != nil {
		c.log.Error("Error while preparing connection file: %v.\n", err)
		return nil, err
	}
	c.log.Debug("Prepared connection info: %v\n", connectionInfo)
	for i := 0; i < len(kernel.Argv); i++ {
		c.log.Debug("spec.Kernel.Argv[%d]: %v", i, kernel.Argv[i])
	}
	// Write connection file and replace placeholders within in command line
	// connectionFileName, err := c.writeConnectionFile(c.configDir, fmt.Sprintf(ConnectionFileFormat, kernelName), connectionInfo)
	// if err != nil {
	// 	c.log.Error("Error while writing connection file: %v.\n", err)
	// 	c.log.Error("Connection info: %v\n", connectionInfo)
	// 	return nil, err
	// }
	// c.log.Debug("Wrote connection file: \"%s\"", connectionFileName)

	jupyterConfigFileInfo, err := c.prepareConfigFileContents(&gateway.KernelReplicaSpec{
		ReplicaId: 1, // TODO(Ben): Is it okay to put 1 here?
		Replicas:  nil,
		Kernel:    kernel,
	})
	if err != nil {
		c.log.Error("Error while preparing config file: %v.\n", err)
		return nil, err
	}

	// configFileName, err := c.writeConfigFile(c.configDir, fmt.Sprintf(ConfigFileFormat, kernelName), jupyterConfigFileInfo)
	// if err != nil {
	// 	c.log.Error("Error while writing config file: %v.\n", err)
	// 	c.log.Error("Config info: %v\n", jupyterConfigFileInfo)
	// 	return nil, err
	// }
	// c.log.Debug("Wrote configuration file: \"%s\"", configFileName)

	// sess := NewSessionDef(kernel.Id, c.nodeLocalMountPoint, c.configDir)

	// TODO(Ben):
	// - I could read in the template files once at the beginning.
	// - I may also be able to do this programmatically (i.e., without reading and writing files) using the Kubernetes Golang client API.

	// Create an empty file. We'll write the populated template for the StatefulSet to this file.
	// statefulSetDefinitionFilePath := filepath.Join(c.configDir, fmt.Sprintf("sess-%s-distr-kernel-statefulset.yaml", kernel.Id))
	// statefulSetDefinitionFile, err := os.Create(statefulSetDefinitionFilePath)
	// defer statefulSetDefinitionFile.Close()

	// Fill out the template for the stateful set.
	// var statefulSetTemplateFile = "./distributed-kernel-stateful-set-template.yaml" // TODO(Ben): Don't hardcode this.
	// statefulSetTemplate, err := template.New("distributed-kernel-stateful-set-template.yaml").ParseFiles(statefulSetTemplateFile)
	// if err != nil {
	// 	panic(err)
	// }
	// err = statefulSetTemplate.Execute(os.Stdout, sess)
	// if err != nil {
	// 	panic(err)
	// }

	// data := KernelConfigMapDataSource{
	// 	SessionId: kernel.Id, ConfigFileInfo: jupyterConfigFileInfo, ConnectionInfo: connectionInfo,
	// }

	// Create an empty file. We'll write the populated template for the ConfigMap to this file.
	// kernelConfigMapDefinitionFilePath := filepath.Join(c.configDir, fmt.Sprintf("kernel-%s-config-map.yaml", kernel.Id))
	// kernelConfigMapDefinitionFile, err := os.Create(kernelConfigMapDefinitionFilePath)
	// defer kernelConfigMapDefinitionFile.Close()

	// Fill out the template for the ConfigMap.
	// var configMapTemplateFile = "./kernel-configmap.yaml" // TODO(Ben): Don't hardcode this.
	// configMapTemplate, err := template.New("kernel-configmap.yaml").ParseFiles(configMapTemplateFile)
	// if err != nil {
	// 	panic(err)
	// }
	// err = configMapTemplate.Execute(kernelConfigMapDefinitionFile, data)
	// if err != nil {
	// 	panic(err)
	// }

	// TODO(Ben): Just marshall this to JSON and then to a string.
	var smr_nodes_buffer bytes.Buffer
	for i := 0; i < len(jupyterConfigFileInfo.SMRNodes); i++ {
		smr_nodes_buffer.WriteString(fmt.Sprintf("\"%s\"", jupyterConfigFileInfo.SMRNodes[i]))
		if (i + 1) < len(jupyterConfigFileInfo.SMRNodes) {
			smr_nodes_buffer.WriteString(",")
		}
	}

	connectionInfoJson, err := json.Marshal(connectionInfo)
	if err != nil {
		panic(err)
	}

	configJson, err := json.Marshal(jupyterConfigFileInfo)
	if err != nil {
		panic(err)
	}

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
	// 			"connection-file.json": fmt.Sprintf(`{
	// 	"shell_port": %d,
	// 	"iopub_port": %d,
	// 	"stdin_port": %d,
	// 	"control_port": %d,
	// 	"hb_port": %d,
	// 	"ip": "0.0.0.0",
	// 	"key": "%s",
	// 	"transport": "tcp",
	// 	"signature_scheme": "%s",
	// 	"kernel_name": ""
	// }`, c.gatewayDaemon.connectionOptions.ShellPort, c.gatewayDaemon.connectionOptions.IOPubPort, c.gatewayDaemon.connectionOptions.StdinPort, c.gatewayDaemon.connectionOptions.ControlPort, c.gatewayDaemon.connectionOptions.HBPort, connectionInfo.Key, connectionInfo.SignatureScheme),
	// 			// "signature-scheme": connectionInfo.SignatureScheme,
	// 			// "key":              connectionInfo.Key,
	// 			// "ip":               "0.0.0.0",
	// 			// "transport":        "tcp",
	// 			// "control-port":     fmt.Sprintf("%d", c.gatewayDaemon.connectionOptions.ControlPort),
	// 			// "shell-port":       fmt.Sprintf("%d", c.gatewayDaemon.connectionOptions.ShellPort),
	// 			// "stdin-port":       fmt.Sprintf("%d", c.gatewayDaemon.connectionOptions.StdinPort),
	// 			// "hbport-port":      fmt.Sprintf("%d", c.gatewayDaemon.connectionOptions.HBPort),
	// 			// "iopub-port":       fmt.Sprintf("%d", c.gatewayDaemon.connectionOptions.IOPubPort),
	// 			// "storage-base": jupyterConfigFileInfo.StorageBase,
	// 			// "smr-node-id":  fmt.Sprintf("%d", jupyterConfigFileInfo.SMRNodeID),
	// 			// "smr-nodes":    strings.Join(jupyterConfigFileInfo.SMRNodes, ","),
	// 			// "smr-join":     strconv.FormatBool(jupyterConfigFileInfo.SMRJoin),
	// 			"ipython_config.json": fmt.Sprintf(`{
	// 	"DistributedKernel": {
	// 		"storage_base": "%s",
	// 		"smr_port": %d,
	// 		"smr_node_id": {replica_id},
	// 		"smr_nodes": [%v],
	// 		"smr_join": %v
	// 	}
	// }`, jupyterConfigFileInfo.StorageBase, c.smrPort, smr_nodes_buffer.String(), strconv.FormatBool(jupyterConfigFileInfo.SMRJoin)),
	// 		},

	// TODO(Ben): Don't hardcode the namespace.
	_, err = c.kubeClientset.CoreV1().ConfigMaps("default").Create(ctx, connectionFileConfigMap, v1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating ConfigMap for connection file for Session %s.", kernel.Id)
		panic(err)
	}

	// 	configFileConfigMap := &corev1.ConfigMap{
	// 		TypeMeta: v1.TypeMeta{
	// 			Kind:       "ConfigMap",
	// 			APIVersion: "v1",
	// 		},
	// 		ObjectMeta: v1.ObjectMeta{
	// 			Name:      fmt.Sprintf("kernel-%s-configfile", kernel.Id),
	// 			Namespace: "default", // TODO(Ben): Don't hardcode the namespace.
	// 		},
	// 		Data: map[string]string{
	// 			"ipython_config.json": fmt.Sprintf(`
	// {
	// 	"DistributedKernel":
	// 	{
	// 		"storage_base":"%s",
	// 		"smr_port":%d,
	// 		"smr_node_id":{{replica_id}},
	// 		"smr_nodes":[%v],
	// 		"smr_join":%v
	// 	}
	// }`, jupyterConfigFileInfo.StorageBase, c.smrPort, strings.Join(jupyterConfigFileInfo.SMRNodes, ","), strconv.FormatBool(jupyterConfigFileInfo.SMRJoin)),
	// 		},
	// 	}

	// 	// TODO(Ben): Don't hardcode the namespace.
	// 	_, err = c.kubeClientset.CoreV1().ConfigMaps("default").Create(ctx, configFileConfigMap, v1.CreateOptions{})
	// 	if err != nil {
	// 		c.log.Error("Error creating ConfigMap for config file for Session %s.", kernel.Id)
	// 		panic(err)
	// 	}

	svcClient := c.kubeClientset.CoreV1().Services(corev1.NamespaceDefault)
	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "tcp",
					Protocol: "TCP",
					Port:     80,
					TargetPort: intstr.IntOrString{
						IntVal: 80,
					},
				},
			},
			Selector: map[string]string{"app": fmt.Sprintf("nginx-session-%s", kernel.Id)},
		},
		ObjectMeta: v1.ObjectMeta{
			Name:   fmt.Sprintf("nginx-session-%s", kernel.Id),
			Labels: map[string]string{"app": fmt.Sprintf("nginx-session-%s", kernel.Id)},
		},
		TypeMeta: v1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
	}
	_, err = svcClient.Create(ctx, svc, v1.CreateOptions{})
	if err != nil {
		c.log.Error("Error creating Service for StatefulSet for Session %s.", kernel.Id)
		panic(err)
	}

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
							"session": fmt.Sprintf("session-%s", kernel.Id),
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
				"session": fmt.Sprintf("session-%s", kernel.Id),
				"app":     fmt.Sprintf("kernel-%s", kernel.Id),
			},
			Name: fmt.Sprintf("kernel-%s", kernel.Id),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{}},
			Selector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"app": fmt.Sprintf("kernel-%s", kernel.Id),
				},
			},
			ServiceName: fmt.Sprintf("nginx-%s", kernel.Id),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: v1.ObjectMeta{
					Labels: map[string]string{
						"session": fmt.Sprintf("session-%s", kernel.Id),
						"app":     fmt.Sprintf("kernel-%s", kernel.Id),
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
						// {
						// 	Name: "kernel-configfile-configmap",
						// 	VolumeSource: corev1.VolumeSource{
						// 		ConfigMap: &corev1.ConfigMapVolumeSource{
						// 			LocalObjectReference: corev1.LocalObjectReference{
						// 				Name: fmt.Sprintf("kernel-%s-configfile", kernel.Id),
						// 			},
						// 		},
						// 	},
						// },
					},
					Containers: []corev1.Container{
						{
							Name:  "kernel",
							Image: "scusemua/jupyter:latest", // TODO(Ben): Don't hardcode this.
							// Command: []string{
							// 	"/bin/sh",
							// },
							// Args: []string{
							// 	"-c", "-i", "\"s/{{replica_id}}/$(echo $POD_NAME | cut -d \"-\" -f 7)/g\"", "/home/jovyan/.ipython/profile_default/ipython_config.json", "&&", "/opt/conda/bin/python3", "-m", "distributed_notebook.kernel", "-f", fmt.Sprintf("kernel-%s-connectionfile-configmap", kernel.Id), "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream",
							// },
							Command: []string{
								"/kernel-entrypoint/kernel-entrypoint.sh",
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8888,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "node-local",
									MountPath: c.nodeLocalMountPoint,
								},
								{
									Name:      "kernel-configmap",
									MountPath: "/kernel-configmap",
									ReadOnly:  false,
								},
								{
									Name:      "kernel-entrypoint",
									MountPath: "/kernel-entrypoint",
									ReadOnly:  false,
								},
								// {
								// 	Name:      "kernel-configfile-configmap",
								// 	MountPath: "/home/jovyan/.ipython/profile_default/ipython_config.json",
								// 	ReadOnly:  true,
								// },
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
									Value: "/kernel-configmap/connection-file.json",
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

	return connectionInfo, nil
}

func (c *BasicKubeClient) writeConnectionFile(dir string, connectionFilePath string, info *jupyter.ConnectionInfo) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, connectionFilePath)
	if err != nil {
		return "", err
	}

	c.log.Debug("Created connection file \"%s\"\n", f.Name())
	c.log.Debug("Writing the following contents to connection file \"%s\": \"%v\"\n", f.Name(), jsonContent)
	f.Write(jsonContent)
	defer f.Close()

	c.log.Debug("Changing permissions of connection file \"%s\" now\n", f.Name())
	if err := os.Chmod(f.Name(), 0777); err != nil {
		log.Fatal(err)
	}

	return f.Name(), nil
}

func (c *BasicKubeClient) writeConfigFile(dir string, configFilePath string, info *jupyter.ConfigFile) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, configFilePath)
	if err != nil {
		return "", err
	}
	c.log.Debug("Created config file \"%s\"\n", f.Name())
	c.log.Debug("Writing the following contents to config file \"%s\": \"%v\"\n", f.Name(), jsonContent)
	f.Write(jsonContent)
	defer f.Close()

	c.log.Debug("Changing permissions of config file \"%s\" now\n", f.Name())
	if err := os.Chmod(f.Name(), 0777); err != nil {
		log.Fatal(err)
	}

	return f.Name(), nil
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
		Transport:       "tcp",
		IP:              "0.0.0.0",
	}

	return connectionInfo, nil
}

func (c *BasicKubeClient) prepareConfigFileContents(spec *gateway.KernelReplicaSpec) (*jupyter.ConfigFile, error) {
	var replicas []string

	// Generate the hostnames for the Pods of the StatefulSet.
	// We can determine them deterministically due to the convention/properties of the StatefulSet.
	for i := 0; i < 3; i++ {
		hostname := fmt.Sprintf("kernel-%s-%d", spec.ID(), i)
		replicas = append(replicas, hostname)
	}

	// Prepare contents of the configuration file.
	file := &jupyter.ConfigFile{
		DistributedKernelConfig: jupyter.DistributedKernelConfig{
			StorageBase: kubeStorageBase,
			SMRNodeID:   int(spec.ReplicaId), // TODO(Ben): Set this to -1 to make it obvious that the Pod needs to fill this in itself?
			SMRNodes:    replicas,
			SMRJoin:     spec.Join,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}
