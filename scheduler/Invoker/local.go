package invoker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	ConnectionFileFormat = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat     = "config-%s-*.json"     // "*" is a placeholder for random string
)

// LocalInvoker Invoke local lambda function simulation
// Use throttle to simulate Lambda network: https://github.com/sitespeedio/throttle
// throttle --up 800000 --down 800000 --rtt 1 (800MB/s, 1ms)
// throttle stop
// Use container to simulate Lambda resouce limit
type LocalInvoker struct {
	cmd      *exec.Cmd
	spec     *gateway.KernelSpec
	closedAt time.Time
	closed   chan struct{}
}

func (ivk *LocalInvoker) InvokeWithContext(ctx context.Context, spec *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec

	// Looking for available port
	connectionInfo, err := ivk.prepareConnectionFile(spec)
	if err != nil {
		return nil, err
	}

	// Write connection file and replace placeholders within in command line
	path, err := ivk.writeConnectionFile("", spec.Id, connectionInfo)
	if err != nil {
		return nil, err
	}
	for i, arg := range spec.Argv {
		spec.Argv[i] = strings.ReplaceAll(arg, "{connection_file}", path)
	}

	// Start kernel process
	log.Printf("Launching kernel \"%s\"\n", strings.Join(spec.Argv, " "))
	if err := ivk.launchKernel(ctx, spec.Id, spec.Argv); err != nil {
		return nil, err
	}
	return connectionInfo, nil
}

func (ivk *LocalInvoker) Status() (jupyter.KernelStatus, error) {
	if ivk.cmd == nil {
		return 0, jupyter.ErrKernelNotLaunched
	} else if ivk.cmd.ProcessState.Exited() {
		return jupyter.KernelStatus(ivk.cmd.ProcessState.ExitCode()), nil
	} else {
		return jupyter.KernelStatusRunning, nil
	}
}

func (ivk *LocalInvoker) Shutdown() error {
	if ivk.cmd == nil {
		return jupyter.ErrKernelNotLaunched
	}

	log.Printf("Signaling  kernel %s...\n", ivk.spec.Id)
	return ivk.cmd.Process.Signal(syscall.SIGINT)
}

func (ivk *LocalInvoker) Close() error {
	if ivk.cmd == nil {
		return jupyter.ErrKernelNotLaunched
	}

	log.Printf("Killing  kernel %s...\n", ivk.spec.Id)
	ivk.cmd.Process.Kill()
	return nil
}

func (ivk *LocalInvoker) Wait() (jupyter.KernelStatus, error) {
	if ivk.cmd == nil {
		return 0, jupyter.ErrKernelNotLaunched
	}

	<-ivk.closed
	ivk.closedAt = time.Time{} // Update closedAt to extend expriation time
	return ivk.Status()
}

func (ivk *LocalInvoker) Expired(timeout time.Duration) bool {
	return ivk.closedAt != time.Time{} && ivk.closedAt.Add(timeout).Before(time.Now())
}

func (ivk *LocalInvoker) prepareConnectionFile(spec *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	// Write connection file
	connectionInfo := &jupyter.ConnectionInfo{
		IP:              "127.0.0.1",
		Transport:       "tcp",
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
	}

	// Reserve ports for the kernel
	socks := make([]net.Listener, 5)
	// Register cleanup for socks
	for i := 0; i < len(socks); i++ {
		// Looking for a random port
		conn, err := net.Listen("tcp", fmt.Sprintf("%s:0", connectionInfo.IP))
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		socks[i] = conn
	}
	// After all sockets are created, assign ports to connectionInfo
	connectionInfo.ControlPort = socks[0].Addr().(*net.TCPAddr).Port
	connectionInfo.ShellPort = socks[1].Addr().(*net.TCPAddr).Port
	connectionInfo.StdinPort = socks[2].Addr().(*net.TCPAddr).Port
	connectionInfo.IOPubPort = socks[3].Addr().(*net.TCPAddr).Port
	connectionInfo.HBPort = socks[4].Addr().(*net.TCPAddr).Port
	return connectionInfo, nil
}

func (ivk *LocalInvoker) prepareConfigFile(spec *gateway.KernelSpec) (*jupyter.ConfigFile, error) {
	return &jupyter.ConfigFile{}, nil
}

func (ivk *LocalInvoker) writeConnectionFile(dir string, id string, info *jupyter.ConnectionInfo) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, fmt.Sprintf(ConnectionFileFormat, id))
	if err != nil {
		return "", err
	}
	f.Write(jsonContent)
	defer f.Close()

	return f.Name(), nil
}

func (ivk *LocalInvoker) writeConfigFile(dir string, id string, info *jupyter.ConfigFile) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, fmt.Sprintf(ConfigFileFormat, id))
	if err != nil {
		return "", err
	}
	f.Write(jsonContent)
	defer f.Close()

	return f.Name(), nil
}

func (ivk *LocalInvoker) launchKernel(ctx context.Context, id string, argv []string) error {
	log.Printf("Starting kernel %s...\n", id)
	ivk.cmd = exec.CommandContext(ctx, argv[0], argv[1:]...)
	ivk.cmd.Stdout = os.Stdout
	ivk.cmd.Stderr = os.Stderr

	if err := ivk.cmd.Start(); err != nil {
		return err
	}

	go func() {
		if err := ivk.cmd.Wait(); err != nil {
			log.Printf("Kernel %s exited with error: %v\n", id, err)
		}
		ivk.closedAt = time.Now()
		close(ivk.closed)
	}()

	return nil
}
