package execution

// Kernel defines a Jupyter kernel with one or more Replica containers associated with it.
type Kernel interface {
	// ID returns the kernel's ID.
	ID() string
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
