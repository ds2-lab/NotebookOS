package proto

import (
	"github.com/zhangjyr/distributed-notebook/common/types"
	"log"
	"reflect"
	"time"
)

// GetGoTimestamp returns the NodeResourcesSnapshot's Timestamp field as a "native" Golang time.Time struct.
// (The NodeResourcesSnapshot's Timestamp struct is of type *timestamppb.Timestamp.)
func (s *NodeResourcesSnapshot) GetGoTimestamp() time.Time {
	return s.Timestamp.AsTime()
}

// Compare compares the object with specified object.
// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
func (s *NodeResourcesSnapshot) Compare(obj interface{}) float64 {
	if obj == nil {
		log.Fatalf("Cannot compare target NodeResourcesSnapshot with nil.")
	}

	other, ok := obj.(types.ArbitraryResourceSnapshot)
	if !ok {
		log.Fatalf("Cannot compare target NodeResourcesSnapshot with specified object of type '%s'.",
			reflect.ValueOf(obj).Type().String())
	}

	if s.GetSnapshotId() < other.GetSnapshotId() {
		return -1
	} else if s.GetSnapshotId() == other.GetSnapshotId() {
		return 0
	} else {
		return 1
	}
}
