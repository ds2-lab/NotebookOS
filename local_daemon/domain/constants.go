package domain

const (
	ShellExecuteReply      = "execute_reply"
	ShellExecuteRequest    = "execute_request"
	ShellYieldExecute      = "yield_execute"
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"

	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/kernel-configmap"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"

	// Passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg = "target_replica"

	YieldInsufficientGPUs         YieldReason = "YieldInsufficientGPUs"         // Yield because there are not enough GPUs available.
	YieldDifferentReplicaTargeted YieldReason = "YieldDifferentReplicaTargeted" // Yield because another replica was explicitly targeted.
)

type YieldReason string
