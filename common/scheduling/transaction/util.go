package transaction

import (
	"fmt"
	"github.com/shopspring/decimal"
)

// getQuantityOfResourceKind returns the (working) field corresponding to the specified Kind of the specified
// *Resources struct.
func getQuantityOfResourceKind(res *Resources, kind Kind) decimal.Decimal {
	switch kind {
	case CPU:
		{
			return res.working.Millicpus
		}
	case Memory:
		{
			return res.working.MemoryMb
		}
	case GPU:
		{
			return res.working.GPUs
		}
	case VRAM:
		{
			return res.working.VRam
		}
	default:
		{
			panic(fmt.Sprintf("invalid/unsupported resource kind: \"%s\"", kind.String()))
		}
	}
}
