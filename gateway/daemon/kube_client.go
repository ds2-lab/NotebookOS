package daemon

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"os"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/configurationFiles/"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"
)

var (
	kubeStorageBase = "/storage" // TODO(Ben): Don't hard-code this. What should this be?
)

type BasicKubeClient struct {
	kubeClientset       *kubernetes.Clientset // Kubernetes client.
	gatewayDaemon       *GatewayDaemon        // Associated Gateway daemon.
	configDir           string                // Where to write config files. This is also where they'll be found on the kernel nodes.
	nodeLocalMountPoint string                // The mount of the shared PVC for all kernel nodes.
	log                 logger.Logger
}

func NewKubeClient() *BasicKubeClient {
	client := &BasicKubeClient{
		configDir:           utils.GetEnv(KubeSharedConfigDir, KubeSharedConfigDir),
		nodeLocalMountPoint: utils.GetEnv(KubeNodeLocalMountPoint, KubeNodeLocalMountPointDefault),
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
func (c *BasicKubeClient) CreateKernelStatefulSet(kernel *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	c.log.Debug("Creating StatefulSet for Session %s.", kernel.Id)

	var templateFile = "./distributed-kernel-deployment-template.yaml"
	tmpl, err := template.New("distributed-kernel-deployment-template.yaml").ParseFiles(templateFile)
	if err != nil {
		panic(err)
	}

	var kernelName string = c.GenerateKernelName(kernel.Id)

	connectionInfo, err := c.prepareConnectionFileContents(kernel)
	if err != nil {
		c.log.Error("Error while preparing connection file: %v.\n", err)
		return nil, err
	}
	c.log.Debug("Prepared connection info: %v\n", connectionInfo)
	// Write connection file and replace placeholders within in command line
	_, err = c.writeConnectionFile(c.configDir, fmt.Sprintf(ConnectionFileFormat, kernelName), connectionInfo)
	if err != nil {
		c.log.Error("Error while writing connection file: %v.\n", err)
		c.log.Error("Connection info: %v\n", connectionInfo)
		return nil, err
	}

	jupyterConfigFileInfo, err := c.prepareConfigFileContents(kernel)
	if err != nil {
		c.log.Error("Error while preparing config file: %v.\n", err)
		return nil, err
	}
	_, err = c.writeConfigFile(c.configDir, fmt.Sprintf(ConfigFileFormat, kernelName), jupyterConfigFileInfo)
	if err != nil {
		c.log.Error("Error while writing config file: %v.\n", err)
		c.log.Error("Config info: %v\n", jupyterConfigFileInfo)
		return nil, err
	}

	sess := NewSessionDef(kernel.Id, c.nodeLocalMountPoint, c.configDir)
	err = tmpl.Execute(os.Stdout, sess)
	if err != nil {
		panic(err)
	}

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
	connectionInfo := &jupyter.ConnectionInfo{
		IP:              "0.0.0.0",
		Transport:       "tcp",
		ControlPort:     c.dockerOpts.ControlPort,
		ShellPort:       c.dockerOpts.ShellPort,
		StdinPort:       c.dockerOpts.StdinPort,
		HBPort:          c.dockerOpts.HBPort,
		IOPubPort:       c.dockerOpts.IOPubPort,
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
	}

	return connectionInfo, nil
}

func (c *BasicKubeClient) prepareConfigFileContents(spec *gateway.KernelReplicaSpec) (*jupyter.ConfigFile, error) {
	// Prepare contents of the configuration file.
	file := &jupyter.ConfigFile{
		DistributedKernelConfig: jupyter.DistributedKernelConfig{
			StorageBase: kubeStorageBase,
			SMRNodeID:   int(spec.ReplicaId),
			SMRNodes:    spec.Replicas,
			SMRJoin:     spec.Join,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}
