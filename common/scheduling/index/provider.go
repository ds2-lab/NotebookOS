package index

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

func GetIndex(policyKey scheduling.PolicyKey, gpusPerHost int) scheduling.ClusterIndex {
	var index scheduling.ClusterIndex
	if policyKey == scheduling.FcfsBatch || policyKey == scheduling.AutoScalingFcfsBatch || policyKey == scheduling.Reservation {
		index = NewRandomClusterIndex(16)
	} else if policyKey == scheduling.Static {
		index = NewStaticClusterIndex(16)
	} else if policyKey == scheduling.DynamicV3 || policyKey == scheduling.DynamicV4 {
		panic("Dynamic v3 and Dynamic v4 are not yet supported.")
	} else if policyKey == scheduling.Gandiva {
		index = NewMultiIndex(int32(gpusPerHost))
	} else {
		panic(fmt.Sprintf("Unknown or unsupported policy \"%s\"; cannot create index", policyKey.String()))
	}

	return index
}
