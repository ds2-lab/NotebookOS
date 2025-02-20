package hashmap

import (
	cmap "github.com/orcaman/concurrent-map/v2"
)

type ConcurrentMap[K comparable, V comparable] struct {
	backend cmap.ConcurrentMap[K, V]
}

func NewConcurrentMap[V comparable](shards int) *ConcurrentMap[string, V] {
	cmap.SHARD_COUNT = shards
	return &ConcurrentMap[string, V]{
		backend: cmap.New[V](),
	}
}

func NewConcurrentMapStringer[K cmap.Stringer, V comparable](shards int) *ConcurrentMap[K, V] {
	cmap.SHARD_COUNT = shards
	return &ConcurrentMap[K, V]{
		backend: cmap.NewStringer[K, V](),
	}
}

func NewConcurrentMapWithCustomShardingFunction[K comparable, V comparable](shards int, sharding func(key K) uint32) *ConcurrentMap[K, V] {
	cmap.SHARD_COUNT = shards
	return &ConcurrentMap[K, V]{
		backend: cmap.NewWithCustomShardingFunction[K, V](sharding),
	}
}

func (m *ConcurrentMap[K, V]) Delete(key K) {
	m.backend.Remove(key)
}

func (m *ConcurrentMap[K, V]) Load(key K) (ret V, ok bool) {
	var val interface{}
	val, ok = m.backend.Get(key)
	ret, _ = val.(V)
	return
}

func (m *ConcurrentMap[K, V]) LoadAndDelete(key K) (retVal V, retExists bool) {
	m.backend.RemoveCb(key, func(key K, val V, exists bool) bool {
		retVal = val
		retExists = exists
		return true
	})
	return
}

func (m *ConcurrentMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	set := m.backend.SetIfAbsent(key, value)
	if set {
		return value, false
	} else {
		return m.Load(key)
	}
}

func (m *ConcurrentMap[K, V]) CompareAndSwap(key K, oldVal V, newVal V) (val V, swapped bool) {
	set := false
	m.backend.Upsert(key, newVal, func(exist bool, valueInMap V, newValue V) V {
		if valueInMap == oldVal {
			set = true
			return newValue
		} else {
			newVal = valueInMap
			return valueInMap
		}
	})
	return newVal, set
}

func (m *ConcurrentMap[K, V]) Range(cb func(K, V) bool) {
	next := true
	for item := range m.backend.IterBuffered() {
		if next {
			next = cb(K(item.Key), item.Val)
		}
		// iterate over all items to drain the channel
	}
}

func (m *ConcurrentMap[K, V]) Store(key K, val V) {
	m.backend.Set(key, val)
}

func (m *ConcurrentMap[K, V]) Len() int {
	return m.backend.Count()
}
