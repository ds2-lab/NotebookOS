package types

import "fmt"

const (
	SMRVersion = "1.0"

	IOTopicSMRReady     = "smr_ready"
	IOTopicSMRNodeAdded = "smr_node_added"
	// IOTopicSMRNodeRemoved = "smr_node_removed"

	MessageTypeSMRLeadTask             = "smr_lead_task"
	MessageTypeAddReplicaRequest       = "add_replica_request"
	MessageTypeUpdateReplicaRequest    = "update_replica_request"
	MessageTypePrepareToMigrateRequest = "prepare_to_migrate_request"
)

type MessageSMRReady struct {
	PersistentID string `json:"persistent_id"`
}

func (m MessageSMRReady) String() string {
	return fmt.Sprintf("MessageSMRReady[PersistentID=%s]", m.PersistentID)
}

type MessageDataDirectory struct {
	KernelId      string `json:"kernel_id"`
	NodeID        int32  `json:"id"`
	DataDirectory string `json:"data_directory"`
	Status        string `json:"status"`
}

func (m MessageDataDirectory) String() string {
	return fmt.Sprintf("MessageDataDirectory[KernelId=%s,Status=%v,DataDirectory=%s,NodeID=%d]", m.KernelId, m.Status, m.DataDirectory, m.NodeID)
}

type MessageSMRNodeUpdated struct {
	MessageSMRReady
	MessageSMRAddOrUpdateReplicaRequest
	KernelId string `json:"kernel_id"`
	Success  bool   `json:"success"`
}

func (m MessageSMRNodeUpdated) String() string {
	return fmt.Sprintf("MessageSMRNodeUpdated[KernelId=%s,Success=%v,PersistentID=%s,NodeID=%d,Address=%s]", m.KernelId, m.Success, m.PersistentID, m.NodeID, m.Address)
}

type MessageSMRLeadTask struct {
	GPURequired bool `json:"gpu"`
}

func (m MessageSMRLeadTask) String() string {
	return fmt.Sprintf("MessageSMRLeadTask[GPURequired=%v]", m.GPURequired)
}

type MessageSMRAddOrUpdateReplicaRequest struct {
	NodeID  int32  `json:"id"`
	Address string `json:"addr"`
}

func (m MessageSMRAddOrUpdateReplicaRequest) String() string {
	return fmt.Sprintf("MessageSMRAddOrUpdateReplicaRequest[NodeID=%d, Address=%s]", m.NodeID, m.Address)
}
