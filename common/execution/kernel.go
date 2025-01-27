package execution

import (
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
)

// Kernel defines a Jupyter kernel with one or more Replica containers associated with it.
type Kernel interface {
	// ID returns the kernel's ID.
	ID() string

	// ReleasePreCommitedResourcesFromReplica is called to release resources that were pre-commited to a
	// Replica before an "execute_request" was forwarded, in order to ensure that the Replica would have
	// the resources available when it received the message.
	ReleasePreCommitedResourcesFromReplica(replica Replica, msg *messaging.JupyterMessage) error
}

// Replica is analogous to scheduling.KernelReplica.
type Replica interface {
	// ID returns the ID of the associated Kernel.
	ID() string

	// ReplicaID returns the SMR node ID of the Replica.
	ReplicaID() int32

	// KernelStoppedTraining is called when the Replica has stopped training.
	KernelStoppedTraining(reason string) error
}
