package core

import (
	"github.com/zhangjyr/distributed-notebook/common/types"
)

// Session defines the interface for a jupyter session.
type MetaSession interface {
	// ID returns the kernel id corresponding to a jupyter session.
	// The ID may change for the same notebook with a in-notebook persistent id.
	ID() string

	// Spec returns the resource specs of the session.
	Spec() types.Spec
}
