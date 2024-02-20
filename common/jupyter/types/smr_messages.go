package types

const (
	SMRVersion = "1.0"

	IOTopicSMRReady     = "smr_ready"
	IOTopicSMRNodeAdded = "smr_node_added"
	// IOTopicSMRNodeRemoved = "smr_node_removed"

	MessageTypeSMRLeadTask             = "smr_lead_task"
	MessageTypeAddReplicaRequest       = "add_replica_request"
	MessageTypePrepareToMigrateRequest = "prepare_to_migrate_request"
)

type MessageSMRReady struct {
	PersistentID string `json:"persistent_id"`
}

type MessageDataDirectory struct {
	KernelId      string `json:"kernel_id"`
	NodeID        int32  `json:"id"`
	DataDirectory string `json:"data-dir"`
}

type MessageSMRNodeUpdated struct {
	MessageSMRReady
	MessageSMRAddReplicaRequest
	KernelId string `json:"kernel_id"`
	Success  bool   `json:"success"`
}

type MessageSMRLeadTask struct {
	GPURequired bool `json:"gpu"`
}

type MessageSMRAddReplicaRequest struct {
	NodeID  int32  `json:"id"`
	Address string `json:"addr"`
}
