package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

type ResourceReservation interface {
	GetReservationId() string
	GetHostId() string
	GetCreationTimestamp() time.Time
	GetCreatedUsingPendingResources() bool
	GetKernelId() string
	GetResourcesReserved() types.Spec
}
