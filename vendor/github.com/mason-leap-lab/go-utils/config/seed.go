package config

import (
	"math/rand"
	"time"
)

type SeedOptions struct {
	Options

	Seed int64 `name:"seed" description:"Random seed to reproduce simulation."`
}

func (opts *SeedOptions) Validate() error {
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	rand.Seed(opts.Seed)
	return nil
}
