package scheduling

import (
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

// RandomPlacer is a simple placer that places sessions randomly.
type RandomPlacer struct {
	AbstractPlacer

	cluster Cluster
	index   *RandomClusterIndex
	opts    *ClusterSchedulerOptions

	mu sync.Mutex
}

// NewRandomPlacer creates a new RandomPlacer.
func NewRandomPlacer(cluster Cluster, opts *ClusterSchedulerOptions) (*RandomPlacer, error) {
	placer := &RandomPlacer{
		cluster: cluster,
		index:   NewRandomClusterIndex(100),
		opts:    opts,
	}

	if err := cluster.AddIndex(placer.index); err != nil {
		return nil, err
	}

	config.InitLogger(&placer.log, placer)
	return placer, nil
}

// FindHosts returns a slice of Host instances that can satisfy the resourceSpec.
func (placer *RandomPlacer) FindHosts(spec types.Spec) []*Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", placer.opts.NumReplicas, spec.String(), placer.index.Len())

	numReplicas := placer.opts.NumReplicas
	if placer.index.Len() == 0 {
		placer.log.Warn(utils.OrangeStyle.Render("Index is empty... returning empty slice of Hosts."))
		return make([]*Host, 0)
	} else if placer.index.Len() < numReplicas {
		placer.log.Warn("Index has just %d hosts (%d are required).", placer.index.Len(), placer.opts.NumReplicas)
		numReplicas = placer.index.Len()
	}

	var (
		pos           interface{}
		candidateHost *Host
		hosts         = make([]*Host, 0, numReplicas)
		failedToLock  = make(map[string]*Host)
	)
	for i := 0; i < numReplicas; i++ {
		candidateHost, pos = placer.index.SeekFrom(pos)
		if locked := candidateHost.TryLockScheduling(); !locked {
			placer.log.Warn("Failed to scheduling-lock host %s due to concurrent scheduling operation.", candidateHost.ID)
			failedToLock[candidateHost.ID] = candidateHost
			continue
		}

		// If the Host can satisfy the resourceSpec, then add it to the slice of Host instances being returned.
		if candidateHost.CanServeContainer(spec) && !candidateHost.WillBecomeTooOversubscribed(spec) {
			hosts = append(hosts, candidateHost)
			placer.log.Debug(utils.GreenStyle.Render("Found viable candidate host: %v. Identified hosts: %d."), candidateHost, len(hosts))
		} else {
			placer.log.Warn(utils.OrangeStyle.Render("Host %v cannot satisfy request %v. (Host resources: %v.)"), candidateHost, candidateHost.ResourceSpec().String(), spec.String())
			candidateHost.UnlockScheduling()
		}
	}

	// If we failed to find enough hosts, then we'll first check if there were any hosts that we couldn't lock due to a
	// concurrent scheduling operation. If so, then we'll wait for ~30 seconds to see if they become available, and we'll
	// try to schedule onto them if they do become available.
	if len(hosts) < numReplicas {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to find %d viable hosts. Found %d/%d."), numReplicas, len(hosts), numReplicas)

		if len(failedToLock) > 0 {
			placer.log.Debug("Failed to scheduling-lock %d candidateHost(s). Will spend 30 seconds waiting to see if they become available...")

			// Try for the next 30 seconds to use any of the hosts that we failed to scheduling-lock.
			st := time.Now()
			for time.Since(st) < time.Second*30 && len(hosts) < numReplicas {
				// If we succeed in locking one of the hosts, then we'll remove it from the mapping so that
				// we don't recheck it (in either case -- that it can serve our new kernel replica or if it cannot).
				removeFromFailedToLock := make([]*Host, 0)

				// Iterate over each of the hosts that we failed to scheduling-lock and retry.
				for _, host := range failedToLock {
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

				time.Sleep(time.Millisecond * 500)
			}
		}
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified %d/%d viable hosts."), len(hosts), numReplicas)
	}

	return hosts
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *RandomPlacer) FindHost(blacklist []interface{}, spec types.Spec) *Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	host, _ := placer.index.Seek(blacklist)

	if host.ResourceSpec().Validate(spec) {
		// The Host can satisfy the resourceSpec, so return it.
		return host
	} else {
		// The Host could not satisfy the resourceSpec, so return nil.
		return nil
	}
}
