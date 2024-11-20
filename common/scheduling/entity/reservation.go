package entity

import (
	"github.com/google/uuid"
	"time"
)

type resourceReservation struct {
	ReservationId                string
	HostId                       string
	CreationTimestamp            time.Time
	CreatedUsingPendingResources bool
}

func newResourceReservation(hostId string, creationTimestamp time.Time, createdUsingPendingResources bool) *resourceReservation {
	reservation := &resourceReservation{
		HostId:                       hostId,
		CreationTimestamp:            creationTimestamp,
		CreatedUsingPendingResources: createdUsingPendingResources,
		ReservationId:                uuid.NewString(),
	}

	return reservation
}
