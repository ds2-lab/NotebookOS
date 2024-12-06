package resource

import "github.com/shopspring/decimal"

const (
	Epsilon = 1.0e-6
)

var (
	EpsilonDecimal = decimal.NewFromFloat(Epsilon)
)

// EqualWithTolerance compares two decimal.Decimal values and returns true if they are equal within
// the tolerance defined by the EpsilonDecimal variable (which is created from the Epsilon constant).
func EqualWithTolerance(d1 decimal.Decimal, d2 decimal.Decimal) bool {
	diff := d1.Sub(d2)
	absDiff := diff.Abs()

	return absDiff.LessThanOrEqual(EpsilonDecimal)
}

// TryRoundToZero accepts a decimal.Decimal as input and returns a decimal.Decimal as its output.
//
// If the given decimal.Decimal is EqualWithTolerance to zero, then TryRoundToZero returns decimal.Zero.
// Otherwise, TryRoundToZero returns the given decimalDecimal (i.e., the original value).
//
// That is, if the given decimal.Decimal is within Epsilon of 0, then TryRoundToZero will return decimal.Zero.
// Otherwise, TryRoundToZero will return the given decimal.Decimal.
func TryRoundToZero(d decimal.Decimal) decimal.Decimal {
	if EqualWithTolerance(d, decimal.Zero) {
		return decimal.Zero
	}

	return d
}
