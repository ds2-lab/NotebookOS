package types

const (
	SMRVersion = "1.0"

	IOTopicSMRReady = "smr_ready"

	MessageTypeSMRReady          = IOTopicSMRReady
	MessageTypeSMRLeadTask       = "smr_lead_task"
	MessageTypeAddReplicaRequest = "add_replica_request"
)

type MessageSMRReady struct {
	PersistentID string `json:"persistent_id"`
}

type MessageSMRLeadTask struct {
	GPURequired bool `json:"gpu"`
}

type MessageSMRAddReplicaRequest struct {
	NodeID  int32  `json:"id"`
	Address string `json:"addr"`
}
