package scheduling

type ResourceUtilization struct {
	// CpuUsage is the % CPU utilization.
	// This value will be between 0 and 100.
	CpuUtilization float64

	// MemoryUsageMb is the amount of RAM used in megabytes, with a minimum value of 0.
	MemoryUsageMb float64

	// AggregateGpuUtilization is the aggregate % GPU utilization across all GPUs.
	// The minimum value of AggregateGpuUtilization is 0.
	// The maximum value of AggregateGpuUtilization is NumGpus * 100.
	AggregateGpuUtilization float64

	// IndividualGpuUtilizationValues is a []float64 containing the individual GPU utilization values for each GPU.
	// The length of IndividualGpuUtilizationValues is equal to NumGpus.
	// The maximum value of IndividualGpuUtilizationValues[i] for i in range 0 to NumGpus is 100.
	IndividualGpuUtilizationValues []float64

	// The number of GPUs currently in-use.
	NumGpus int
}

func NewResourceUtilization(cpuUtil float64, memUsageMb float64, gpuUtils []float64) *ResourceUtilization {
	util := &ResourceUtilization{
		CpuUtilization:                 cpuUtil,
		MemoryUsageMb:                  memUsageMb,
		NumGpus:                        len(gpuUtils),
		IndividualGpuUtilizationValues: gpuUtils,
	}

	util.AggregateGpuUtilization = 0.0
	for _, gpuUtil := range gpuUtils {
		util.AggregateGpuUtilization += gpuUtil
	}

	return util
}

func NewEmptyResourceUtilization() *ResourceUtilization {
	return &ResourceUtilization{}
}

func (u *ResourceUtilization) WithCpuUtilization(util float64) *ResourceUtilization {
	u.CpuUtilization = util
	return u
}

func (u *ResourceUtilization) WithMemoryUsageMb(mb float64) *ResourceUtilization {
	u.MemoryUsageMb = mb
	return u
}

func (u *ResourceUtilization) WithGpuUtilizationValues(utils []float64) *ResourceUtilization {
	u.NumGpus = len(utils)
	u.IndividualGpuUtilizationValues = utils

	u.AggregateGpuUtilization = 0.0
	for _, util := range utils {
		u.AggregateGpuUtilization += util
	}

	return u
}
