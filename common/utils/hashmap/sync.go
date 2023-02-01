package hashmap

import (
	"sync"
)

type SyncMap[K any, V any] struct {
	backend sync.Map
}

func NewSyncMap[K any, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.backend.Delete(key)
}

func (m *SyncMap[K, V]) Load(key K) (ret V, ok bool) {
	v, ok := m.backend.Load(key)
	ret, _ = v.(V)
	return
}

func (m *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := m.backend.LoadAndDelete(key)
	if loaded {
		value, _ = v.(V)
	}
	return
}

func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded := m.backend.LoadOrStore(key, value)
	actual, _ = v.(V)
	return
}

// Just a dummy implementation to that will fail always.
func (m *SyncMap[K, V]) CompareAndSwap(key K, old V, new V) (interface{}, bool) {
	v, _ := m.Load(key)
	return v, false
}

func (m *SyncMap[K, V]) Range(cb func(K, V) bool) {
	m.backend.Range(func(key any, value any) bool {
		v, _ := value.(V)
		return cb(key.(K), v)
	})
}

func (m *SyncMap[K, V]) Store(key K, val V) {
	m.backend.Store(key, val)
}
