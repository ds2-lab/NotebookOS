package index

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// GetIndex returns a (pointer to a) concrete struct implementing the scheduling.ClusterIndex interface.
//
// The policyKey is used to determine which struct should be created and returned.
//
// The numPools parameter is used when the scheduling policy indicates that a MultiIndex should be returned.
func GetIndex(policyKey scheduling.PolicyKey, numPools int) scheduling.ClusterIndex {
	var (
		index scheduling.ClusterIndex
		err   error
	)
	if policyKey == scheduling.FcfsBatch || policyKey == scheduling.AutoScalingFcfsBatch || policyKey == scheduling.Reservation {
		index = NewRandomClusterIndex(16)
	} else if policyKey == scheduling.Static {
		index = NewStaticClusterIndex(16)
	} else if policyKey == scheduling.DynamicV3 || policyKey == scheduling.DynamicV4 {
		panic("Dynamic v3 and Dynamic v4 are not yet supported.")
	} else if policyKey == scheduling.Gandiva {
		index, err = NewMultiIndex[*LeastLoadedIndex](int32(numPools), NewLeastLoadedIndexWrapper)
	} else {
		panic(fmt.Sprintf("Unknown or unsupported policy \"%s\"; cannot create index", policyKey.String()))
	}

	if err != nil {
		panic(err)
	}

	return index
}
