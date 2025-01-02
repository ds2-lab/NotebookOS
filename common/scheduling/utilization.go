package scheduling

type Utilization interface {
	String() string
	StringFormatted() string
	GetCpuUtilization() float64
	GetMemoryUsageMb() float64
	GetVramUsageGb() float64
	GetAggregateGpuUtilization() float64
	GetIndividualGpuUtilizationValues() []float64
	GetNumGpus() int
}
