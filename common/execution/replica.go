package execution

// ActiveReplica is analogous to scheduling.KernelReplica.
type ActiveReplica interface {
	ID() string
	ReplicaID() int32
	KernelStoppedTraining(reason string) error
}
