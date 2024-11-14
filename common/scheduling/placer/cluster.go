package placer

import (
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/index"
)

type Cluster interface {
	MetricsProvider() scheduling.MetricsProvider

	// AddIndex adds an index to the BaseCluster. For each category and expected value, there can be only one index.
	AddIndex(index index.ClusterIndexProvider) error
}
