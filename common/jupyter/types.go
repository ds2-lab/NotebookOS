package jupyter

import (
	"encoding/json"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"strings"
)

var (
	ErrNotSupported       = fmt.Errorf("not supported")
	ErrNoCancelConfigured = fmt.Errorf("this request was not configured with a context that supported cancellation")
	ErrKernelNotLaunched  = fmt.Errorf("kernel not launched")
	ErrKernelNotReady     = fmt.Errorf("kernel not ready")
	ErrKernelClosed       = fmt.Errorf("kernel closed")
	ErrNoHandler          = fmt.Errorf("no handler")
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

	if s < 0 {
		if s == KernelStatusInitializing {
			return "Unknown(KernelStatusInitializing)"
		} else if s == KernelStatusAbnormal {
			return "Unknown(KernelStatusAbnormal)"
		} else if s == KernelStatusRunning {
			return "Unknown(KernelStatusRunning)"
		}

		return fmt.Sprintf("Unknown(%d)", s)
	}

	return [...]string{"Running", "Initializing", "Exited"}[s]
}

// ConnectionInfo stores the contents of the kernel connection info.
// This is not used by Kernels directly, as it has two fields `IOPubPort` and `IOSubPort`
// that are used to configure other components, such as the Gateway and LocalDaemons.
// We convert this struct to a `ConnectionInfoForKernel` when we want to pass configuration to a kernel.
// The definition is compatible with github.com/Scusemua/go-utils/config.SchedulerOptions
type ConnectionInfo struct {
	IP                   string `json:"ip" name:"ip" description:"The IP address of the kernel."`
	ControlPort          int    `json:"control_port" name:"control-port" description:"The port for control messages."`
	ShellPort            int    `json:"shell_port" name:"shell-port" description:"The port for shell messages."`
	StdinPort            int    `json:"stdin_port" name:"stdin-port" description:"The port for stdin messages."`
	HBPort               int    `json:"hb_port" name:"hb-port" description:"The port for heartbeat messages."`
	IOPubPort            int    `json:"iopub_port" name:"iopub-port" description:"The port for iopub messages on the kernel (for the pub socket). In clients, we'll create a SUB socket using this to connect to the kernel's PUB socket."`
	IOSubPort            int    `json:"iosub_port" name:"iosub-port" description:"The port for iopub messages (for the sub socket)."`
	AckPort              int    `json:"ack_port" name:"ack-port" description:"The port to use for the ACK socket."`
	Transport            string `json:"transport" name:"transport"`
	SignatureScheme      string `json:"signature_scheme"`
	Key                  string `json:"key"`
	StartingResourcePort int    `json:"starting_resource_port" name:"starting-resource-port" description:"The first 'resource port'. Resource ports are the ports exposed by the Kubernetes services that are available for ZMQ sockets to listen on."`
	NumResourcePorts     int    `json:"num_resource_ports" name:"num-resource-ports" description:"The total number of available resource ports. If the 'starting-port' is 9006 and there are 20 resource ports, then the following ports are available: 9006, 9007, 9008, ..., 9024, 9025, 9026. Resource ports are the ports exposed by the Kubernetes services that are available for ZMQ sockets to listen on."`
}

func (info *ConnectionInfo) String() string {
	m, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (info *ConnectionInfo) PrettyString(indentSize int) string {
	indentBuilder := strings.Builder{}
	for i := 0; i < indentSize; i++ {
		indentBuilder.WriteString(" ")
	}

	m, err := json.MarshalIndent(info, "", indentBuilder.String())
	if err != nil {
		panic(err)
	}

	return string(m)
}

// ConnectionInfoFromKernelConnectionInfo converts the given *proto.KernelConnectionInfo to a *ConnectionInfo.
func ConnectionInfoFromKernelConnectionInfo(ci *proto.KernelConnectionInfo) *ConnectionInfo {
	return &ConnectionInfo{
		IP:              ci.Ip,
		ControlPort:     int(ci.ControlPort),
		ShellPort:       int(ci.ShellPort),
		StdinPort:       int(ci.StdinPort),
		IOSubPort:       int(ci.IosubPort),
		IOPubPort:       int(ci.IopubPort),
		HBPort:          int(ci.HbPort),
		Transport:       ci.Transport,
		SignatureScheme: ci.SignatureScheme,
		Key:             ci.Key,
	}
}

func (info *ConnectionInfo) ToConnectionInfoForKernel() *ConnectionInfoForKernel {
	return &ConnectionInfoForKernel{
		IP:              info.IP,
		ControlPort:     info.ControlPort,
		ShellPort:       info.ShellPort,
		StdinPort:       info.StdinPort,
		HBPort:          info.HBPort,
		IOPubPort:       info.IOPubPort,
		Transport:       info.Transport,
		SignatureScheme: info.SignatureScheme,
		Key:             info.Key,
	}
}

// ConnectionInfoForKernel stores the contents of the kernel connection info.
// The definition is compatible with github.com/Scusemua/go-utils/config.SchedulerOptions
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
	StorageBase                          string               `json:"storage_base"`
	SMRPort                              int                  `json:"smr_port"`
	SMRNodeID                            int                  `json:"smr_node_id"`
	SMRNodes                             []string             `json:"smr_nodes"`
	SMRJoin                              bool                 `json:"smr_join"`
	PersistentID                         string               `json:"persistent_id,omitempty"`
	RemoteStorageEndpoint                string               `json:"remote_storage_hostname"`
	RemoteStorage                        string               `json:"remote_storage"`
	RegisterWithLocalDaemon              bool                 `json:"should_register_with_local_daemon"`
	LocalDaemonAddr                      string               `json:"local_daemon_addr"` // Only used in Docker mode.
	SpecCpus                             float64              `json:"spec_cpus"`
	SpecMemoryMb                         float64              `json:"spec_mem_mb"`
	SpecGpus                             int                  `json:"spec_gpus"`
	SpecVramGb                           float64              `json:"spec_vram_gb"`
	DeploymentMode                       types.DeploymentMode `json:"deployment_mode"`
	SimulateCheckpointingLatency         bool                 `json:"simulate_checkpointing_latency"`
	ElectionTimeoutSeconds               int                  `json:"election_timeout_seconds"`
	SimulateWriteAfterExec               bool                 `json:"simulate_write_after_execute"`                  // Simulate network write after executing code?
	SimulateWriteAfterExecOnCriticalPath bool                 `json:"simulate_write_after_execute_on_critical_path"` // Should the simulated network write after executing code be on the critical path?
	WorkloadId                           string               `json:"workload_id"`
	SmrEnabled                           bool                 `json:"smr_enabled"`
	PrometheusServerPort                 int                  `json:"prometheus_port"` // PrometheusServerPort is the port of the Prometheus metrics server on/in each kernel replica container.
}

func (c DistributedKernelConfig) String() string {
	m, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return string(m)
	//return fmt.Sprintf("StorageBase: %s, SMRPort: %d, SMRNodeID: %d, SMRJoin: %v, PersistentID: %s, SMRNodes: %s, RemoteStorage: %s, RemoteStorageEndpoint: %s",
	//	c.StorageBase, c.SMRPort, c.SMRNodeID, c.SMRJoin, c.PersistentID, strings.Join(c.SMRNodes, ","), c.RemoteStorage, c.RemoteStorageEndpoint)
}

type ConfigFile struct {
	DistributedKernelConfig `json:"DistributedKernel"`
}

func (c ConfigFile) String() string {
	return c.DistributedKernelConfig.String()
}
