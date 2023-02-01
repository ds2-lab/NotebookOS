package types

import "fmt"

var (
	ErrKernelNotLaunched = fmt.Errorf("kernel not launched")
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
	IOPubPort       int
	Transport       string
	SignatureScheme string
	Key             string
}
