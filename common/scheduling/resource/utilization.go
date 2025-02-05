package resource

import (
	"encoding/json"
)

type Utilization struct {

	// IndividualGpuUtilizationValues is a []float64 containing the individual GPU utilization values for each GPU.
	// The length of IndividualGpuUtilizationValues is equal to NumGpus.
	// The maximum value of IndividualGpuUtilizationValues[i] for i in range 0 to NumGpus is 100.
	IndividualGpuUtilizationValues []float64 `json:"individual_gpu_utilization_values"`

	// CpuUsage is the % CPU utilization.
	// This value will be between 0 and 100.
	CpuUtilization float64 `json:"cpu_utilization"`

	// MemoryUsageMb is the amount of RAM used in megabytes, with a minimum value of 0.
	MemoryUsageMb float64 `json:"memory_utilization_mb"`

	// Vram usage is the amount of VRAM being used in gigabytes, with a minimum value of 0.
	VramUsageGb float64 `json:"vram_utilization_gb"`

	// AggregateGpuUtilization is the aggregate % GPU utilization across all GPUs.
	// The minimum value of AggregateGpuUtilization is 0.
	// The maximum value of AggregateGpuUtilization is NumGpus * 100.
	AggregateGpuUtilization float64 `json:"aggregate_gpu_utilization"`

	// The number of GPUs currently in-use.
	NumGpus int `json:"num_gpus"`
}

func NewUtilization(cpuUtil float64, memUsageMb float64, gpuUtils []float64, vramGb float64) *Utilization {
	util := &Utilization{
		CpuUtilization:                 cpuUtil,
		MemoryUsageMb:                  memUsageMb,
		NumGpus:                        len(gpuUtils),
		IndividualGpuUtilizationValues: gpuUtils,
		VramUsageGb:                    vramGb,
	}

	util.AggregateGpuUtilization = 0.0
	for _, gpuUtil := range gpuUtils {
		util.AggregateGpuUtilization += gpuUtil
	}

	return util
}

func (u *Utilization) NumGpusAsFloat() float64 {
	return float64(u.NumGpus)
}

func NewEmptyUtilization() *Utilization {
	return &Utilization{}
}

func (u *Utilization) WithCpuUtilization(util float64) *Utilization {
	u.CpuUtilization = util
	return u
}

func (u *Utilization) WithMemoryUsageMb(mb float64) *Utilization {
	u.MemoryUsageMb = mb
	return u
}

func (u *Utilization) WithVramUtilizationGb(gb float64) *Utilization {
	u.VramUsageGb = gb
	return u
}

// WithNGpuUtilizationValues populates the Utilization struct's IndividualGpuUtilizationValues field
// with a slice of length n, where each element of that slice has value util.
//
// Likewise, WithNGpuUtilizationValues sets the Utilization struct's AggregateGpuUtilization field
// to n * util and the NumGpus field to n.
func (u *Utilization) WithNGpuUtilizationValues(n int32, util float64) *Utilization {
	u.NumGpus = int(n)
	u.AggregateGpuUtilization = float64(n) * util
	u.IndividualGpuUtilizationValues = make([]float64, 0, n)

	var i int32
	for i = 0; i < n; i++ {
		u.IndividualGpuUtilizationValues = append(u.IndividualGpuUtilizationValues, util)
	}

	return u
}

func (u *Utilization) WithGpuUtilizationValues(utils []float64) *Utilization {
	u.NumGpus = len(utils)
	u.IndividualGpuUtilizationValues = utils

	u.AggregateGpuUtilization = 0.0
	for _, util := range utils {
		u.AggregateGpuUtilization += util
	}

	return u
}

func (u *Utilization) String() string {
	m, err := json.Marshal(u)
	if err != nil {
		panic(err)
	}

	return string(m)
}

func (u *Utilization) StringFormatted() string {
	m, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		panic(err)
	}

	return string(m)
}

// GetCpuUtilization returns the CPU utilization percentage.
func (u *Utilization) GetCpuUtilization() float64 {
	return u.CpuUtilization
}

// GetMemoryUsageMb returns the amount of memory used in megabytes.
func (u *Utilization) GetMemoryUsageMb() float64 {
	return u.MemoryUsageMb
}

// GetVramUsageGb returns the amount of VRAM used in gigabytes.
func (u *Utilization) GetVramUsageGb() float64 {
	return u.VramUsageGb
}

// GetAggregateGpuUtilization returns the aggregate GPU utilization percentage.
func (u *Utilization) GetAggregateGpuUtilization() float64 {
	return u.AggregateGpuUtilization
}

// GetIndividualGpuUtilizationValues returns the individual GPU utilization values.
func (u *Utilization) GetIndividualGpuUtilizationValues() []float64 {
	return u.IndividualGpuUtilizationValues
}

// GetNumGpus returns the number of GPUs currently in use.
func (u *Utilization) GetNumGpus() int {
	return u.NumGpus
}
