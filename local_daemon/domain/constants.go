package domain

const (
	KubeSharedConfigDir        = "SHARED_CONFIG_DIR"
	KubeSharedConfigDirDefault = "/kernel-configmap"

	KubeNodeLocalMountPoint        = "NODE_LOCAL_MOUNT_POINT"
	KubeNodeLocalMountPointDefault = "/data"

	// TargetReplicaArg is used as a key within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg = "target_replica"

	YieldInsufficientResourcesAvailable YieldReason = "YieldInsufficientResourcesAvailable" // Yield because there are not enough resources (Millicpus, memory, and/or GPUs) available.
	YieldDifferentReplicaTargeted       YieldReason = "YieldDifferentReplicaTargeted"       // Yield because another replica was explicitly targeted.
	YieldExplicitlyInstructed           YieldReason = "YieldExplicitlyInstructed"           // Yield because we were explicitly instructed to yield.
	YieldUnspecifiedReason              YieldReason = "YieldUnspecifiedReason"              // Yield for some other reason.
)

type YieldReason string
