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

func (u *ResourceUtilization) NumGpusAsFloat() float64 {
	return float64(u.NumGpus)
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

// WithNGpuUtilizationValues populates the ResourceUtilization struct's IndividualGpuUtilizationValues field
// with a slice of length n, where each element of that slice has value util.
//
// Likewise, WithNGpuUtilizationValues sets the ResourceUtilization struct's AggregateGpuUtilization field
// to n * util and the NumGpus field to n.
func (u *ResourceUtilization) WithNGpuUtilizationValues(n int32, util float64) *ResourceUtilization {
	u.NumGpus = int(n)
	u.AggregateGpuUtilization = float64(n) * util
	u.IndividualGpuUtilizationValues = make([]float64, 0, n)

	var i int32
	for i = 0; i < n; i++ {
		u.IndividualGpuUtilizationValues = append(u.IndividualGpuUtilizationValues, util)
	}

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
