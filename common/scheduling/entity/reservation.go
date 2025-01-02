package entity

import (
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

type Reservation struct {
	ReservationId                string
	HostId                       string
	KernelId                     string
	CreationTimestamp            time.Time
	CreatedUsingPendingResources bool
	ResourcesReserved            types.Spec
}

func NewReservation(hostId string, kernelId string, creationTimestamp time.Time, createdUsingPendingResources bool, resourcesReserved types.Spec) *Reservation {
	reservation := &Reservation{
		HostId:                       hostId,
		KernelId:                     kernelId,
		CreationTimestamp:            creationTimestamp,
		CreatedUsingPendingResources: createdUsingPendingResources,
		ReservationId:                uuid.NewString(),
		ResourcesReserved:            resourcesReserved,
	}

	return reservation
}

func (r *Reservation) GetResourcesReserved() types.Spec {
	return r.ResourcesReserved
}

func (r *Reservation) GetKernelId() string {
	return r.KernelId
}

func (r *Reservation) GetReservationId() string {
	return r.GetReservationId()
}

func (r *Reservation) GetHostId() string {
	return r.HostId
}

func (r *Reservation) GetCreationTimestamp() time.Time {
	return r.CreationTimestamp
}

func (r *Reservation) GetCreatedUsingPendingResources() bool {
	return r.CreatedUsingPendingResources
}
