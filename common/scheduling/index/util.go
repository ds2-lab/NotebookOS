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
