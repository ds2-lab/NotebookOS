package scheduling

const (
	// MillicpusPerHost is the number of CPU resources available on each host for
	// allocation to kernel replicas in millicpus (1/1000th of a vCPU).
	MillicpusPerHost = 8000

	// MemoryMbPerHost is the amount of memory (i.e., RAM) available on each host for
	// allocation to kernel replicas in megabytes (MB).
	MemoryMbPerHost = 16384

	// VramPerHostGb is the amount of VRAM (i.e., video memory, GPU memory, etc.) available on each host for
	// allocation to kernel replicas in gigabytes (GB).
	VramPerHostGb float64 = 40.0
)
