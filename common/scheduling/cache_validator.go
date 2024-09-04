package scheduling

import (
	"github.com/mason-leap-lab/go-utils/cache"
	"time"
)

// GetClockTimeCacheValidator create a hacky ICValidator that can be used to validate
// cached value by comparing  the current time and cachedAt in the function closure.
//
// Call InlineCache.Validator(ClockTime) to update cachedAt.
func GetClockTimeCacheValidator() cache.ICValidator {
	cachedAt := time.Time{}
	return func(cached interface{}) bool {
		if ts, ok := cached.(time.Time); ok {
			cachedAt = ts
			return false
		} else {
			return time.Now().Equal(cachedAt)
		}
	}
}
