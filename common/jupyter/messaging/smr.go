package messaging

import "fmt"

const (
	SMRVersion = "1.0"

	IOTopicSMRReady     = "smr_ready"
	IOTopicSMRNodeAdded = "smr_node_added"
	// IOTopicSMRNodeRemoved = "smr_node_removed"

	MessageTypeErrorReport             = "error_report"
	MessageTypeSMRLeadTask             = "smr_lead_task"
	MessageTypeLeadAfterYield          = "smr_lead_after_yield" // Used by kernels to report an error that they've been selected to lead despite yielding.
	MessageTypeAddReplicaRequest       = "add_replica_request"
	MessageTypeUpdateReplicaRequest    = "update_replica_request"
	MessageTypePrepareToMigrateRequest = "prepare_to_migrate_request"
	MessageTypeStartSyncLogRequest     = "start_synclog_request"
)

type MessageSMRReady struct {
	PersistentID string `json:"persistent_id"`
}

func (m MessageSMRReady) String() string {
	return fmt.Sprintf("MessageSMRReady[PersistentID=%s]", m.PersistentID)
}

type MessageDataDirectory struct {
	KernelId      string `json:"kernel_id"`
	DataDirectory string `json:"data_directory"`
	Status        string `json:"status"`
	NodeID        int32  `json:"id"`
}

func (m MessageDataDirectory) String() string {
	return fmt.Sprintf("MessageDataDirectory[kernelId=%s,Status=%v,DataDirectory=%s,HostId=%d]", m.KernelId, m.Status, m.DataDirectory, m.NodeID)
}

type MessageSMRNodeUpdated struct {
	MessageSMRReady
	KernelId string `json:"kernel_id"`
	MessageSMRAddOrUpdateReplicaRequest
	Success bool `json:"success"`
}

func (m MessageSMRNodeUpdated) String() string {
	return fmt.Sprintf("MessageSMRNodeUpdated[kernelId=%s,Success=%v,PersistentID=%s,HostId=%d,Address=%s]", m.KernelId, m.Success, m.PersistentID, m.NodeID, m.Address)
}

type MessageSMRLeadTask struct {

	// ExecuteRequestMsgId is the Jupyter msg_id (from the header) of the "execute_request"
	// message that was used to submit the code execution request.
	ExecuteRequestMsgId string `json:"execute_request_msg_id"`

	// UnixMilliseconds is the Unix epoch time in milliseconds at which the "smr_lead_task" notification
	// message was created (and thus approximates when it was sent and when the kernel began executing
	// the user's code).
	UnixMilliseconds int64 `json:"msg_created_at_unix_milliseconds"`

	GPURequired bool `json:"gpu"`
}

type MessageSMRLeadAfterYield struct {
	Term int `json:"term"`
}

func (m MessageSMRLeadTask) String() string {
	return fmt.Sprintf("MessageSMRLeadTask[GPURequired=%v]", m.GPURequired)
}

type MessageSMRAddOrUpdateReplicaRequest struct {
	Address string `json:"addr"`
	NodeID  int32  `json:"id"`
}

func (m MessageSMRAddOrUpdateReplicaRequest) String() string {
	return fmt.Sprintf("MessageSMRAddOrUpdateReplicaRequest[HostId=%d, Address=%s]", m.NodeID, m.Address)
}

type ErrorReport struct {
	ErrorTitle   string `json:"error"`
	ErrorMessage string `json:"message"`
	KernelId     string `json:"kernel_id"`
}
