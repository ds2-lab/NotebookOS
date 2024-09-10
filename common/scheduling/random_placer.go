package scheduling

import (
	"sync"

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
		pos   interface{}
		host  *Host
		hosts = make([]*Host, 0, numReplicas)
	)
	placer.log.Debug("Inital length of hosts: %d", len(hosts))
	for i := 0; i < numReplicas; i++ {
		// TODO: Is this implementation really correct?
		// placer.log.Debug("Seeking viable host from position %d...", pos)
		host, pos = placer.index.SeekFrom(pos)
		// placer.log.Debug("Found host: %v. Next position: %d.", host, pos)

		// If the Host can satisfy the resourceSpec, then add it to the slice of Host instances being returned.
		if host.ResourceSpec().Validate(spec) {
			hosts = append(hosts, host)
			placer.log.Debug(utils.GreenStyle.Render("Found viable host: %v. Identified hosts: %d."), host, len(hosts))
		} else {
			placer.log.Warn(utils.OrangeStyle.Render("Host %v cannot satisfy request %v. (Host resources: %v.)"), host, host.ResourceSpec().String(), spec.String())
		}
	}

	if len(hosts) < numReplicas {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to find %d viable hosts. Found %d/%d."), numReplicas, len(hosts), numReplicas)
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
