package utils

import (
	"github.com/shopspring/decimal"
	"math/rand"
	"os"
	"time"
	"unsafe"
)

const (
	Epsilon       = 1.0e-6
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var (
	EpsilonDecimal = decimal.NewFromFloat(Epsilon)
)

func ContextKey(name string) interface{} {
	return &name
}

func GetEnv(name string, def string) string {
	val := os.Getenv(name)
	if len(val) > 0 {
		return val
	} else {
		return def
	}
}

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

func TryRoundToDecimal(d decimal.Decimal, roundTo decimal.Decimal) decimal.Decimal {
	if EqualWithTolerance(d, roundTo) {
		return roundTo
	}

	return d
}

// GenerateRandomString generates a random string of length n.
func GenerateRandomString(n int) string {
	var src = rand.NewSource(time.Now().UnixNano())

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
