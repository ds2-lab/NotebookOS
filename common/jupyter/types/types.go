package types

import (
	"fmt"
	"strings"
)

var (
	ErrNotSupported                = fmt.Errorf("not supported")
	ErrKernelNotLaunched           = fmt.Errorf("kernel not launched")
	ErrKernelNotReady              = fmt.Errorf("kernel not ready")
	ErrKernelClosed                = fmt.Errorf("kernel closed")
	ErrInvalidJupyterMessage       = fmt.Errorf("invalid jupyter message")
	ErrNotSupportedSignatureScheme = fmt.Errorf("not supported signature scheme")
	ErrInvalidJupyterSignature     = fmt.Errorf("invalid jupyter signature")

	ErrStopPropagation = fmt.Errorf("stop propagation")
	ErrNoHandler       = fmt.Errorf("no handler")
)

const (
	KernelStatusInitializing KernelStatus = iota - 3
	KernelStatusAbnormal
	KernelStatusRunning
	KernelStatusExited
	KernelStatusError
)

type KernelStatus int32

func (s KernelStatus) String() string {
	if s >= KernelStatusError {
		return fmt.Sprintf("Error(%d)", s)
	}

	return [...]string{"Running", "Initializing", "Exited"}[s]
}

// ConnectionInfo stores the contents of the kernel connection info.
// This is not used by Kernels directly, as it has two fields `IOPubPort` and `IOSubPort`
// that are used to configure other components, such as the Gateway and LocalDaemons.
// We convert this struct to a `ConnectionInfoForKernel` when we want to pass configuration to a kernel.
// The definition is compatible with github.com/mason-leap-lab/go-utils/config.Options
type ConnectionInfo struct {
	IP                   string `json:"ip" name:"ip" description:"The IP address of the kernel."`
	ControlPort          int    `json:"control_port" name:"control-port" description:"The port for control messages."`
	ShellPort            int    `json:"shell_port" name:"shell-port" description:"The port for shell messages."`
	StdinPort            int    `json:"stdin_port" name:"stdin-port" description:"The port for stdin messages."`
	HBPort               int    `json:"hb_port" name:"hb-port" description:"The port for heartbeat messages."`
	IOPubPort            int    `json:"iopub_port" name:"iopub-port" description:"The port for iopub messages on the kernel (for the pub socket). In clients, we'll create a SUB socket using this to connect to the kernel's PUB socket."`
	IOSubPort            int    `json:"iosub_port" name:"iosub-port" description:"The port for iopub messages (for the sub socket)."`
	Transport            string `json:"transport"`
	SignatureScheme      string `json:"signature_scheme"`
	Key                  string `json:"key"`
	StartingResourcePort int    `json:"starting_resource_port" name:"starting-resource-port" description:"The first 'resource port'. Resource ports are the ports exposed by the Kubernetes services that are available for ZMQ sockets to listen on."`
	NumResourcePorts     int    `json:"num_resource_ports" name:"num-resource-ports" description:"The total number of available resource ports. If the 'starting-port' is 9006 and there are 20 resource ports, then the following ports are available: 9006, 9007, 9008, ..., 9024, 9025, 9026. Resource ports are the ports exposed by the Kubernetes services that are available for ZMQ sockets to listen on."`
}

func (ci ConnectionInfo) String() string {
	return fmt.Sprintf("IP: %s, ControlPort: %d, ShellPort: %d, StdinPort: %d, HBPort: %d, IOPubPort: %d, IOSubPort: %d, Transport: %s, SignatureScheme: %s, Key: %s", ci.IP, ci.ControlPort, ci.ShellPort, ci.StdinPort, ci.HBPort, ci.IOPubPort, ci.IOSubPort, ci.Transport, ci.SignatureScheme, ci.Key)
}

func (ci ConnectionInfo) ToConnectionInfoForKernel() *ConnectionInfoForKernel {
	return &ConnectionInfoForKernel{
		IP:              ci.IP,
		ControlPort:     ci.ControlPort,
		ShellPort:       ci.ShellPort,
		StdinPort:       ci.StdinPort,
		HBPort:          ci.HBPort,
		IOPubPort:       ci.IOPubPort,
		Transport:       ci.Transport,
		SignatureScheme: ci.SignatureScheme,
		Key:             ci.Key,
	}
}

// ConnectionInfo stores the contents of the kernel connection info.
// The definition is compatible with github.com/mason-leap-lab/go-utils/config.Options
type ConnectionInfoForKernel struct {
	IP              string `json:"ip" name:"ip" description:"The IP address of the kernel."`
	ControlPort     int    `json:"control_port" name:"control-port" description:"The port for control messages."`
	ShellPort       int    `json:"shell_port" name:"shell-port" description:"The port for shell messages."`
	StdinPort       int    `json:"stdin_port" name:"stdin-port" description:"The port for stdin messages."`
	HBPort          int    `json:"hb_port" name:"hb-port" description:"The port for heartbeat messages."`
	IOPubPort       int    `json:"iopub_port" name:"iopub-port" description:"The port for iopub messages on the kernel."`
	Transport       string `json:"transport"`
	SignatureScheme string `json:"signature_scheme"`
	Key             string `json:"key"`
}

func (ci ConnectionInfoForKernel) String() string {
	return fmt.Sprintf("IP: %s, ControlPort: %d, ShellPort: %d, StdinPort: %d, HBPort: %d, IOPubPort: %d, Transport: %s, SignatureScheme: %s, Key: %s", ci.IP, ci.ControlPort, ci.ShellPort, ci.StdinPort, ci.HBPort, ci.IOPubPort, ci.Transport, ci.SignatureScheme, ci.Key)
}

type DistributedKernelConfig struct {
	StorageBase          string   `json:"storage_base"`
	SMRPort              int      `json:"smr_port"`
	SMRNodeID            int      `json:"smr_node_id"`
	SMRNodes             []string `json:"smr_nodes"`
	SMRJoin              bool     `json:"smr_join"`
	PersistentID         string   `json:"persistent_id,omitempty"`
	HDFSNameNodeEndpoint string   `json:"hdfs_namenode_hostname"`
}

func (c DistributedKernelConfig) String() string {
	return fmt.Sprintf("StorageBase: %s, SMRPort: %d, SMRNodeID: %d, SMRJoin: %v, PersistentID: %s, SMRNodes: %s", c.StorageBase, c.SMRPort, c.SMRNodeID, c.SMRJoin, c.PersistentID, strings.Join(c.SMRNodes, ","))
}

type ConfigFile struct {
	DistributedKernelConfig `json:"DistributedKernel"`
}

func (c ConfigFile) String() string {
	return c.DistributedKernelConfig.String()
}
