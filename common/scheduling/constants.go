package scheduling

const (
	// DefaultMillicpusPerHost is the number of CPU TransactionResources available on each host for
	// allocation to kernel replicas in millicpus (1/1000th of a vCPU).
	DefaultMillicpusPerHost = 64000

	// DefaultMemoryMbPerHost is the amount of memory (i.e., RAM) available on each host for
	// allocation to kernel replicas in megabytes (MB).
	DefaultMemoryMbPerHost = 128000

	// DefaultVramPerHostGb is the amount of VRAM (i.e., video memory, GPU memory, etc.) available on each host for
	// allocation to kernel replicas in gigabytes (GB).
	DefaultVramPerHostGb float64 = 40.0

	DefaultScalingIntervalSeconds = 30
	DefaultMaxSubscribedRatio     = 7.0
)
