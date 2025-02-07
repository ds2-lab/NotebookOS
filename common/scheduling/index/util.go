package index

import "github.com/scusemua/distributed-notebook/common/scheduling"

func ContainsHost(hosts []scheduling.Host, target scheduling.Host) bool {
	if len(hosts) == 0 {
		return false
	}

	if target == nil {
		return false
	}

	for _, host := range hosts {
		if host.GetID() == target.GetID() {
			return true
		}
	}

	return false
}

// getBlacklist converts the list of interface{} to a list of []int32 containing
// the indices of blacklisted host instances within a MultiIndex.
func getBlacklist(blacklist []interface{}) []scheduling.Host {
	__blacklist := make([]scheduling.Host, 0)
	for _, meta := range blacklist {
		if meta == nil {
			continue
		}

		__blacklist = append(__blacklist, meta.(scheduling.Host))
	}

	return __blacklist
}
