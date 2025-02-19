package invoker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/utils"
)

const (
	ConnectionFileFormat = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat     = "config-%s-*.json"     // "*" is a placeholder for random string
)

// NOTE: As of right now, the "LocalInvoker" is *not* related to the "LocalMode" deployment mode.

// LocalInvoker invokes local jupyter kernel
// Use throttle to simulate Lambda network: https://github.com/sitespeedio/throttle
// throttle --up 800000 --down 800000 --rtt 1 (800MB/s, 1ms)
// throttle stop
// kernel replica is not supported so far. Add if needed.
type LocalInvoker struct {
	// closedAt is the time at which the KernelInvoker closed or stopped its kernel.
	closedAt time.Time

	// createdAt is the time at which the KernelInvoker first created its kernel.
	createdAt time.Time

	log logger.Logger

	// connInfo is the Jupyter connection info used to connect/communicate with the kernel.
	connInfo *jupyter.ConnectionInfo

	cmd           *exec.Cmd
	spec          *proto.KernelReplicaSpec
	closed        chan struct{}
	statusChanged StatucChangedHandler

	// kernelId is the ID of the target kernel.
	kernelId string

	// workloadId is the ID of the workload in which the LocalInvoker's kernel exists
	workloadId string

	// id uniquely identifies this Invoker instance.
	id string

	// remoteStorageEndpoint is the network endpoint of the remote storage.
	remoteStorageEndpoint string

	// remoteStorage indicates the type of remote storage, either 'hdfs' or 'redis'
	remoteStorage string

	// DeploymentMode is the deployment mode of the cluster
	DeploymentMode types.DeploymentMode

	// S3Bucket is the AWS S3 bucket name if we're using AWS S3 for our remote storage.
	S3Bucket string

	// AwsRegion is the AWS region in which to create/look for the S3 bucket (if we're using AWS S3 for remote storage).
	AwsRegion string

	// RedisPassword is the password to access Redis (only relevant if using Redis for remote storage).
	RedisPassword string

	// RedisPort is the port of the Redis server (only relevant if using Redis for remote storage).
	RedisPort int

	// SMRPort is the network port used by the SMR cluster.
	SMRPort int

	// RedisDatabase is the database number to use (only relevant if using Redis for remote storage).
	RedisDatabase int

	AssignedGpuDeviceIds                 []int32 // AssignedGpuDeviceIds is the list of GPU device IDs that are being assigned to the kernel replica that we are invoking. Note that if SimulateTrainingUsingSleep is true, then this option is ultimately ignored.
	KernelDebugPort                      int32   // Debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	electionTimeoutSeconds               int     // electionTimeoutSeconds is how long kernel leader elections wait to receive all proposals before deciding on a leader
	prometheusMetricsPort                int     // prometheusMetricsPort is the port that the container should serve prometheus metrics on.
	simulateCheckpointingLatency         bool    // simulateCheckpointingLatency controls whether the kernels will be configured to simulate the latency of performing checkpointing after a migration (read) and after executing code (write).
	simulateWriteAfterExec               bool    // Simulate network write after executing code?
	simulateWriteAfterExecOnCriticalPath bool    // Should the simulated network write after executing code be on the critical path?
	SimulateTrainingUsingSleep           bool    // SimulateTrainingUsingSleep controls whether we tell the kernels to train using real GPUs and real PyTorch code or not.
	BindGPUs                             bool    // BindGPUs indicates whether we should bind GPUs to the container or not. We can still train with CPU-PyTorch, so we only want to bind GPUs if we are going to be using real GPUs.
	BindAllGpus                          bool    // BindAllGpus instructs the DockerInvoker to bind ALL GPUs to the container when creating it (if SimulateTrainingUsingSleep is false). Note that if SimulateTrainingUsingSleep is true, then this option is ultimately ignored.
	BindDebugpyPort                      bool    // BindDebugpyPort specifies whether to bind a port to kernel containers for DebugPy
	SaveStoppedKernelContainers          bool    // If true, then do not fully remove stopped kernel containers.
	SmrEnabled                           bool    // SmrEnabled indicates if SMR is enabled.
	RetrieveDatasetsFromS3               bool    // RetrieveDatasetsFromS3 is a bool flag that, when true, instructs the KernelInvoker to configure the kernels to retrieve datasets from an S3 bucket.
	DatasetsS3Bucket                     string  // DatasetsS3Bucket is the S3 bucket from which the kernels retrieve the datasets when RetrieveDatasetsFromS3 is set to true.

	status jupyter.KernelStatus

	created bool
}

func (ivk *LocalInvoker) WorkloadId() string {
	return ivk.workloadId
}

func (ivk *LocalInvoker) SetWorkloadId(workloadId string) {
	ivk.workloadId = workloadId
}

func (ivk *LocalInvoker) GetAssignedGpuDeviceIds() []int32 {
	return ivk.AssignedGpuDeviceIds
}

func (ivk *LocalInvoker) SetAssignedGpuDeviceIds(assignedGpuDeviceIds []int32) {
	ivk.AssignedGpuDeviceIds = assignedGpuDeviceIds
}

func (ivk *LocalInvoker) DebugPort() int32 {
	return ivk.KernelDebugPort
}

func (ivk *LocalInvoker) SetDebugPort(kernelDebugPort int32) {
	ivk.KernelDebugPort = kernelDebugPort
}

func (ivk *LocalInvoker) KernelId() string {
	return ivk.kernelId
}

func (ivk *LocalInvoker) SetKernelId(kernelId string) {
	ivk.kernelId = kernelId
}

func NewLocalInvoker() *LocalInvoker {
	invoker := &LocalInvoker{}
	invoker.statusChanged = invoker.defaultStatusChangedHandler

	config.InitLogger(&invoker.log, invoker)

	return invoker
}

func (ivk *LocalInvoker) InvokeWithContext(ctx context.Context, spec *proto.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec
	ivk.status = jupyter.KernelStatusInitializing
	if ivk.statusChanged == nil {
		ivk.statusChanged = ivk.defaultStatusChangedHandler
	}

	ivk.log.Debug("[LocalInvoker] Invoking with context now.")

	// Looking for available port
	connectionInfo, err := ivk.prepareConnectionFile(spec.Kernel)
	if err != nil {
		ivk.log.Debug("Error while preparing connection file: %v.", err)
		return nil, ivk.reportLaunchError(err)
	}

	// Write connection file and replace placeholders within in command line
	path, err := ivk.writeConnectionFile("", spec.Kernel.Id, connectionInfo)
	if err != nil {
		ivk.log.Debug("Error while writing connection file: %v.", err)
		ivk.log.Debug("Connection info: %v", connectionInfo)
		return nil, ivk.reportLaunchError(err)
	}
	for i, arg := range spec.Kernel.Argv {
		spec.Kernel.Argv[i] = strings.ReplaceAll(arg, "{connection_file}", path)
	}

	// Start kernel process
	ivk.log.Debug("Launching kernel \"%s\"", strings.Join(spec.Kernel.Argv, " "))
	if err := ivk.launchKernel(ctx, spec.Kernel.Id, spec.Kernel.Argv); err != nil {
		return nil, ivk.reportLaunchError(err)
	}

	ivk.setStatus(jupyter.KernelStatusRunning)
	return connectionInfo, nil
}

func (ivk *LocalInvoker) Status() (jupyter.KernelStatus, error) {
	if ivk.cmd == nil {
		return 0, jupyter.ErrKernelNotLaunched
	} else {
		return ivk.status, nil
	}
}

func (ivk *LocalInvoker) Shutdown() error {
	if ivk.cmd == nil {
		return jupyter.ErrKernelNotLaunched
	}

	ivk.log.Debug("Signaling  kernel %s...", ivk.spec.Kernel.Id)
	return ivk.cmd.Process.Signal(syscall.SIGINT)
}

func (ivk *LocalInvoker) Close() error {
	if ivk.cmd == nil {
		return jupyter.ErrKernelNotLaunched
	}

	ivk.log.Debug("Killing  kernel %s...", ivk.spec.Kernel.Id)
	err := ivk.cmd.Process.Kill()
	if err != nil {
		ivk.log.Error("Error while attempting to kill process: %v", err)
		return err
	}

	return nil
}

func (ivk *LocalInvoker) Wait() (jupyter.KernelStatus, error) {
	if ivk.cmd == nil {
		return 0, jupyter.ErrKernelNotLaunched
	}

	<-ivk.closed
	ivk.closedAt = time.Time{} // Update closedAt to extend expiration time
	return ivk.Status()
}

func (ivk *LocalInvoker) Expired(timeout time.Duration) bool {
	return ivk.closedAt != time.Time{} && ivk.closedAt.Add(timeout).Before(time.Now())
}

func (ivk *LocalInvoker) OnStatusChanged(handler StatucChangedHandler) {
	ivk.statusChanged = handler
}

func (ivk *LocalInvoker) GetReplicaAddress(_ *proto.KernelSpec, _ int32) string {
	ivk.initSMRPort()
	return fmt.Sprintf("127.0.0.1:%d", ivk.SMRPort)
}

// initSMRPort initialize SMR port with environment variable
func (ivk *LocalInvoker) initSMRPort() {
	if ivk.SMRPort == 0 {
		ivk.SMRPort, _ = strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	}
	if ivk.SMRPort == 0 {
		ivk.SMRPort = KernelSMRPortDefault
	}
}

func (ivk *LocalInvoker) prepareConnectionFile(spec *proto.KernelSpec) (*jupyter.ConnectionInfo, error) {
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
		// Can we just call this directly? Or do we not actually want to close it...?
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

func (ivk *LocalInvoker) writeConnectionFile(dir string, name string, info *jupyter.ConnectionInfo) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		ivk.log.Error("Failed to marshal connection info because: %v", err)
		return "", err
	}

	targetDirForLogging := dir
	if targetDirForLogging == "" {
		targetDirForLogging = os.TempDir()
	}

	ivk.log.Debug("Creating temporary file \"%s\" in directory \"%s\" to contain kernel connection info",
		fmt.Sprintf(ConnectionFileFormat, name), targetDirForLogging)

	f, err := os.CreateTemp(dir, fmt.Sprintf(ConnectionFileFormat, name))
	if err != nil {
		ivk.log.Error("CreateTemp(\"%s\", \"%s\") failed because: %v", targetDirForLogging, fmt.Sprintf(ConnectionFileFormat, name), err)
		return "", err
	}

	ivk.log.Debug("Created connection file \"%s\" in directory \"%s\"", f.Name(), targetDirForLogging)
	ivk.log.Debug("Writing the following contents to connection file \"%s\": \"%v\"", f.Name(), string(jsonContent))
	_, err = f.Write(jsonContent)
	if err != nil {
		ivk.log.Error("Failed to write JSON-encoded connection info to file \"%s\" because: %v", f.Name(), err)
		return "", err
	}

	defer f.Close()

	ivk.log.Debug("Changing permissions of connection file \"%s\" now", f.Name())
	if err := os.Chmod(f.Name(), 0777); err != nil {
		log.Fatal(err)
	}

	return f.Name(), nil
}

func (ivk *LocalInvoker) writeConfigFile(dir string, name string, info *jupyter.ConfigFile) (string, error) {
	jsonContent, err := json.Marshal(info)
	if err != nil {
		ivk.log.Error("Failed to marshal config file struct because: %v", err)
		return "", err
	}

	targetDirForLogging := dir
	if targetDirForLogging == "" {
		targetDirForLogging = os.TempDir()
	}

	ivk.log.Debug("Creating temporary file \"%s\" in directory \"%s\" to contain kernel config info",
		fmt.Sprintf(ConnectionFileFormat, name), targetDirForLogging)

	f, err := os.CreateTemp(dir, fmt.Sprintf(ConfigFileFormat, name))
	if err != nil {
		ivk.log.Error("CreateTemp(\"%s\", \"%s\") failed because: %v", targetDirForLogging, fmt.Sprintf(ConnectionFileFormat, name), err)
		return "", err
	}
	ivk.log.Debug("Created config file \"%s\"", f.Name())
	ivk.log.Debug("Writing the following contents to config file \"%s\": \"%v\"", f.Name(), string(jsonContent))
	_, err = f.Write(jsonContent)
	if err != nil {
		ivk.log.Error("Failed to write JSON-encoded config info to file \"%s\" because: %v", f.Name(), err)
		return "", err
	}

	defer f.Close()

	ivk.log.Debug("Changing permissions of config file \"%s\" now", f.Name())
	if err := os.Chmod(f.Name(), 0777); err != nil {
		log.Fatal(err)
	}

	return f.Name(), nil
}

func (ivk *LocalInvoker) launchKernel(ctx context.Context, id string, argv []string) error {
	ivk.log.Debug("Starting kernel %s...", id)
	ivk.cmd = exec.CommandContext(ctx, argv[0], argv[1:]...)
	ivk.cmd.Stdout = os.Stdout
	ivk.cmd.Stderr = os.Stderr

	if err := ivk.cmd.Start(); err != nil {
		return err
	}

	go func() {
		if err := ivk.cmd.Wait(); err != nil {
			ivk.log.Debug("kernel %s exited with error: %v\n", id, err)
		}
		ivk.closedAt = time.Now()
		close(ivk.closed)
		ivk.setStatus(jupyter.KernelStatus(ivk.cmd.ProcessState.ExitCode()))
		// Status will not change anymore, reset the handler.
		ivk.statusChanged = ivk.defaultStatusChangedHandler
	}()

	ivk.created = true
	ivk.createdAt = time.Now()

	return nil
}

func (ivk *LocalInvoker) reportLaunchError(err error) error {
	ivk.setStatus(jupyter.KernelStatusAbnormal)
	// Status will not change anymore, reset the handler.
	ivk.statusChanged = ivk.defaultStatusChangedHandler
	return err
}

func (ivk *LocalInvoker) defaultStatusChangedHandler(_ jupyter.KernelStatus, _ jupyter.KernelStatus) {
	// Do nothing
}

func (ivk *LocalInvoker) setStatus(status jupyter.KernelStatus) {
	var old jupyter.KernelStatus
	old, ivk.status = ivk.status, status
	if old != ivk.status {
		ivk.statusChanged(old, ivk.status)
	}
}

// KernelCreatedAt returns the time at which the LocalInvoker created the kernel.
func (ivk *LocalInvoker) KernelCreatedAt() (time.Time, bool) {
	if !ivk.created {
		return time.Time{}, false
	}

	return ivk.createdAt, true
}

// KernelCreated returns a bool indicating whether kernel the container has been created.
func (ivk *LocalInvoker) KernelCreated() bool {
	return ivk.created
}

// TimeSinceKernelCreated returns the amount of time that has elapsed since the LocalInvoker created the kernel.
func (ivk *LocalInvoker) TimeSinceKernelCreated() (time.Duration, bool) {
	if !ivk.created {
		return time.Duration(-1), false
	}

	return time.Since(ivk.createdAt), true
}
