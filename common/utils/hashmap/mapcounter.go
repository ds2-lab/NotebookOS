package hashmap

import "sync/atomic"

type MapCounter[K any, V any] struct {
	BaseHashMap[K, V]
	size int64
}

func (m *MapCounter[K, V]) Delete(key K) {
	m.LoadAndDelete(key)
}

func (m *MapCounter[K, V]) LoadAndDelete(key K) (retVal V, retExists bool) {
	retVal, retExists = m.BaseHashMap.LoadAndDelete(key)
	if retExists {
		atomic.AddInt64(&m.size, -1)
	}
	return
}

func (m *MapCounter[K, V]) LoadOrStore(key K, value V) (val V, loaded bool) {
	val, loaded = m.BaseHashMap.LoadOrStore(key, value)
	if !loaded {
		atomic.AddInt64(&m.size, 1)
	}
	return val, loaded
}

func (m *MapCounter[K, V]) Store(key K, val V) {
	_, loaded := m.LoadOrStore(key, val)
	if loaded {
		m.BaseHashMap.Store(key, val)
	}
}

func (m *MapCounter[K, V]) Len() int {
	return int(atomic.LoadInt64(&m.size))
}
