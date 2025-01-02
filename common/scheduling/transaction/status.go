package transaction

const (
	// IdleResources can overlap with pending HostResources. These are HostResources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources Status = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending HostResources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending HostResources on a node is the amount of HostResources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources Status = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources Status = "committed"

	// SpecResources are the total allocatable HostResources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources Status = "spec"
)

// Status differentiates between idle, pending, committed, and spec HostResources.
type Status string

func (t Status) String() string {
	return string(t)
}
