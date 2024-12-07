package transaction

const (
	// NoResource is a sort of default value for Kind.
	NoResource Kind = "N/A"
	CPU        Kind = "CPU"
	GPU        Kind = "GPU"
	VRAM       Kind = "VRAM"
	Memory     Kind = "Memory"
)

// Kind can be one of CPU, GPU, or Memory
type Kind string

func (k Kind) String() string {
	return string(k)
}
