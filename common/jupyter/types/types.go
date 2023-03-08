package types

import "fmt"

var (
	ErrNotSupported      = fmt.Errorf("not supported")
	ErrKernelNotLaunched = fmt.Errorf("kernel not launched")
	ErrKernelNotReady    = fmt.Errorf("kernel not ready")
	ErrKernelClosed      = fmt.Errorf("kernel closed")
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
// The definition is compatible with github.com/mason-leap-lab/go-utils/config.Options
type ConnectionInfo struct {
	IP              string `json:"ip" name:"ip" description:"The IP address of the kernel."`
	ControlPort     int    `json:"control_port" name:"control-port" description:"The port for control messages."`
	ShellPort       int    `json:"shell_port" name:"shell-port" description:"The port for shell messages."`
	StdinPort       int    `json:"stdin_port" name:"stdin-port" description:"The port for stdin messages."`
	HBPort          int    `json:"hb_port" name:"hb-port" description:"The port for heartbeat messages."`
	IOPubPort       int    `json:"iopub_port" name:"iopub-port" description:"The port for iopub messages."`
	Transport       string `json:"transport"`
	SignatureScheme string `json:"signature_scheme"`
	Key             string `json:"key"`
}

type DistributedKernelConfig struct {
	StorageBase string   `json:"storage_base"`
	SMRPort     int      `json:"smr_port"`
	SMRNodeID   int      `json:"smr_node_id"`
	SMRNodes    []string `json:"smr_nodes"`
}

type ConfigFile struct {
	DistributedKernelConfig `json:"DistributedKernel"`
}
