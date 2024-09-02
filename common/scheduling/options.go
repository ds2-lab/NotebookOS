package scheduling

import "fmt"

type CoreOptions struct {
	NumReplicas                 int     `name:"num-replicas" description:"Number of kernel replicas."`
	MaxSubscribedRatio          float64 `name:"max-subscribed-ratio" description:"Maximum subscribed ratio."`
	ExecutionTimeSamplingWindow int64   `name:"execution-time-sampling-window" description:"Window size for moving average of training time. Specify a negative value to compute the average as the average of ALL execution times."`
	MigrationTimeSamplingWindow int64   `name:"migration-time-sampling-window" description:"Window size for moving average of migration time. Specify a negative value to compute the average as the average of ALL migration times."`
}

func (co CoreOptions) String() string {
	return fmt.Sprintf("NumReplicas: %d, MaxSubscribedRatio: %.4f", co.NumReplicas, co.MaxSubscribedRatio)
}
