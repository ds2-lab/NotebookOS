package sync

import "context"

type NilPool[V any] struct {
	New    func() V
	Closer func(V)
}

func (p *NilPool[V]) Get(_ context.Context) (v V, err error) {
	if p.New == nil {
		return
	} else {
		return p.New(), nil
	}
}

func (p *NilPool[V]) Release(v V) {
	if p.Closer != nil {
		p.Closer(v)
	}
}

func (p *NilPool[V]) Put(v V) error {
	p.Release(v)
	return nil
}

func (p *NilPool[V]) Close() {
}
