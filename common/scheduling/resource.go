package scheduling

const (
	// NoResource is a sort of default value for ResourceKind.
	NoResource ResourceKind = "N/A"
	CPU        ResourceKind = "CPU"
	GPU        ResourceKind = "GPU"
	VRAM       ResourceKind = "VRAM"
	Memory     ResourceKind = "Memory"
)

// ResourceKind can be one of CPU, GPU, or Memory
type ResourceKind string

func (k ResourceKind) String() string {
	return string(k)
}
