package scheduling

import (
	"time"

	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

// RandomPlacer is a simple placer that places sessions randomly.
type RandomPlacer struct {
	*AbstractPlacer

	index *RandomClusterIndex
}

// NewRandomPlacer creates a new RandomPlacer.
func NewRandomPlacer(cluster clusterInternal, opts *ClusterSchedulerOptions) (*RandomPlacer, error) {
	basePlacer := newAbstractPlacer(cluster, opts)
	randomPlacer := &RandomPlacer{
		AbstractPlacer: basePlacer,
		index:          NewRandomClusterIndex(100),
	}

	if err := cluster.AddIndex(randomPlacer.index); err != nil {
		return nil, err
	}

	basePlacer.instance = randomPlacer
	return randomPlacer, nil
}

// index returns the ClusterIndex of the specific Placer implementation.
func (placer *RandomPlacer) getIndex() ClusterIndex {
	return placer.index
}

// hostIsViable returns a tuple (bool, bool).
// First bool represents whether the host is viable.
// Second bool indicates whether the host was successfully locked. This does not mean that it is still locked.
// Merely that we were able to lock it when we tried. If we locked it and found that the host wasn't viable,
// then we'll have unlocked it before hostIsViable returns.
func (placer *RandomPlacer) hostIsViable(candidateHost *Host, spec types.Spec) (bool, bool) {
	// Attempt to lock the host for our scheduling operation.
	// If we fail to lock it, then we'll make note if that and try again later (if necessary).
	// It is currently involved in another scheduling operation and may be available once that operation completes.
	if locked := candidateHost.TryLockScheduling(); !locked {
		placer.log.Warn("Failed to scheduling-lock host %s due to concurrent scheduling operation.", candidateHost.ID)
		return false, false
	}

	// If the Host can satisfy the resourceSpec, then add it to the slice of Host instances being returned.
	if candidateHost.ResourceSpec().Validate(spec) && candidateHost.CanServeContainer(spec) && !candidateHost.WillBecomeTooOversubscribed(spec) {
		// The Host can satisfy the resource request. Keep the host locked and return true.
		placer.log.Debug(utils.GreenStyle.Render("Found viable candidate host: %v."), candidateHost)
		return true, true
	} else {
		// The Host could not satisfy the resource request. Unlock it and return false.
		placer.log.Warn(utils.OrangeStyle.Render("Host %v cannot satisfy request %v. (Host resources: %v.)"), candidateHost, candidateHost.ResourceSpec().String(), spec.String())
		candidateHost.UnlockScheduling()
		return false, true
	}
}

// FindHosts returns a slice of Host instances that can satisfy the resourceSpec.
func (placer *RandomPlacer) findHosts(spec types.Spec) []*Host {
	numReplicas := placer.opts.NumReplicas

	var (
		pos          interface{} = nil
		hosts        []*Host     = nil
		failedToLock             = make(map[string]*Host)
	)

	// Seek `numReplicas` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numReplicas, func(candidateHost *Host) bool {
		// Check if the host is viable.
		viable, locked := placer.hostIsViable(candidateHost, spec)
		if viable {
			// It's viable, so return true.
			return true
		}

		// It wasn't viable. Did we simply fail to lock it (and therefore couldn't check its viability)?
		if !locked {
			// Simply couldn't check the host's viability. We'll try again later (if we need to).
			failedToLock[candidateHost.ID] = candidateHost
		}

		// Return false because the host ultimately wasn't viable for one reason (truly not viable) or another (we
		// simply couldn't lock the host and check if it is viable or not).
		return false
	}, make([]interface{}, 0))
	placer.mu.Unlock()

	// If we failed to find enough hosts, then we'll first check if there were any hosts that we couldn't lock due to a
	// concurrent scheduling operation. If so, then we'll wait for ~30 seconds to see if they become available, and we'll
	// try to schedule onto them if they do become available.
	if len(hosts) < numReplicas {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to find %d viable hosts. Found %d/%d."), numReplicas, len(hosts), numReplicas)

		// Were there any hosts we couldn't test due to their being involved in a separate, concurrent scheduling operation?
		if len(failedToLock) > 0 {
			// There were some hosts we couldn't check before.
			// We'll spend up to a minute trying to lock them for our scheduling operation before aborting.
			interval := time.Second * 60
			placer.log.Debug("Failed to scheduling-lock %d candidateHost(s). "+
				"Will spend %v waiting to see if they become available...", interval)

			// Try for the next `interval` to use any of the hosts that we failed to scheduling-lock.
			st := time.Now()
			for time.Since(st) < interval && len(hosts) < numReplicas {
				placer.mu.Lock()
				// If we succeed in locking one of the hosts, then we'll remove it from the mapping so that
				// we don't recheck it (in either case -- that it can serve our new kernel replica or if it cannot).
				removeFromFailedToLock := make([]*Host, 0)

				// Iterate over each of the hosts that we failed to scheduling-lock and retry.
				for _, host := range failedToLock {
					if !host.IsContainedWithinIndex {
						placer.log.Warn("Host %s is no longer in a ClusterIndex. Must have been removed.", host.ID)
						removeFromFailedToLock = append(removeFromFailedToLock, host)
						continue
					}

					// Try to scheduling-lock the host.
					if locked := host.TryLockScheduling(); locked {
						// If we locked it, then check if it is viable. If it is, then we'll use it.
						if host.CanServeContainer(spec) && !host.WillBecomeTooOversubscribed(spec) {
							hosts = append(hosts, host)
							removeFromFailedToLock = append(removeFromFailedToLock, host)
							placer.log.Debug(utils.GreenStyle.Render("Locked and found viable candidate host: %s. Identified hosts: %d."), host.ID, len(hosts))

							// If we've found enough hosts, then we can stop iterating now.
							if len(hosts) == numReplicas {
								placer.log.Debug(utils.GreenStyle.Render("Successfully identified %d/%d viable hosts after retrying hosts we originally failed to lock."), len(hosts), numReplicas)
								break
							}
						} else {
							// Host wasn't viable. Unlock it, and remove it from the mapping.
							placer.log.Warn(utils.OrangeStyle.Render("Finally locked host %s, but host cannot satisfy request %v. (Host resources: %v.)"), host.ID, host.ResourceSpec().String(), spec.String())
							host.UnlockScheduling()
							removeFromFailedToLock = append(removeFromFailedToLock, host)
						}
					} else {
						// TODO: Remove this eventually; it'll print way too many times.
						placer.log.Warn("Once again failed to scheduling-lock host %s due to concurrent scheduling operation.", host.ID)
					}
				}

				// Remove any hosts that we either locked and found that they could serve our kernel replica,
				// or we locked them and found they couldn't serve our kernel replica.
				//
				// We only bother with this step if we've still not found enough hosts.
				if len(hosts) < numReplicas {
					// Iterate over each of the hosts that we were able to lock and remove it from the mapping.
					for _, host := range removeFromFailedToLock {
						delete(failedToLock, host.ID)
					}
				}

				placer.mu.Unlock()
				time.Sleep(time.Millisecond * 500)
			}
		}
	}

	return hosts
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *RandomPlacer) findHost(blacklist []interface{}, spec types.Spec) *Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost *Host) bool {
		viable, _ := placer.hostIsViable(candidateHost, spec)
		return viable
	}, blacklist)

	if len(hosts) > 0 {
		return hosts[0]
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}
