package types

import "fmt"

var (
	ErrKernelNotLaunched = fmt.Errorf("kernel not launched")
	ErrKernelReady       = fmt.Errorf("kernel not ready")
	ErrKernelClosed      = fmt.Errorf("kernel closed")
)

const (
	KernelStatusInitializing KernelStatus = iota - 3
	KernelStatusAbnormal
	KernelStatusRunning
	KernelStatusExited
	KernelStatusError
)

type KernelStatus int

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
	ControlPort     int    `json:"control_port" name:"control_port" description:"The port for control messages."`
	ShellPort       int    `json:"shell_port" name:"shell_port" description:"The port for shell messages."`
	StdinPort       int    `json:"stdin_port" name:"stdin_port" description:"The port for stdin messages."`
	HBPort          int    `json:"hb_port" name:"hb_port" description:"The port for heartbeat messages."`
	IOPubPort       int    `json:"iopub_port" name:"iopub_port" description:"The port for iopub messages."`
	Transport       string `json:"transport"`
	SignatureScheme string `json:"signature_scheme"`
	Key             string `json:"key"`
}

type DistributedKernelConfig struct {
	StorageBase string `json:"storage_base"`
}

type ConfigFile struct {
	DistributedKernelConfig `json:"DistributedKernel"`
}
