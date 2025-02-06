package hashmap

import (
	"fmt"
	"github.com/zhangjyr/hashmap"
	"log"
	"reflect"
	"sync"
)

type ThreadsafeCornelkMap[K any, V any] struct {
	hashmap   *hashmap.HashMap
	stringKey bool

	mu sync.RWMutex
}

func NewThreadsafeCornelkMap[K any, V any](size int) *ThreadsafeCornelkMap[K, V] {
	var key K
	return &ThreadsafeCornelkMap[K, V]{
		stringKey: reflect.TypeOf(key).Kind() == reflect.String,
		hashmap:   hashmap.New((uintptr)(size)),
	}
}

func (m *ThreadsafeCornelkMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.hashmap.Del(key)
}

func (m *ThreadsafeCornelkMap[K, V]) Load(key K) (ret V, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.get(key)
	if v != nil {
		ret, ok = v.(V)
		if !ok {
			log.Panicf("ThreadsafeCornelkMap.Load: type mismatch %v\n", v)
			panic(fmt.Sprintf("ThreadsafeCornelkMap.Load: type mismatch %v\n", v))
		}
	}
	return ret, ok
}

func (m *ThreadsafeCornelkMap[K, V]) LoadAndDelete(key K) (ret V, retExists bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, retExists := m.get(key)
	if !retExists {
		return ret, retExists
	} else if v == deleted {
		return ret, false
	}

	for !m.hashmap.Cas(key, v, deleted) {
		v, retExists = m.get(key)
		if !retExists {
			return ret, retExists
		} else if v == deleted {
			return ret, false
		}
	}

	if v != nil {
		ret = v.(V)
	}
	m.hashmap.Del(key)
	return ret, retExists
}

func (m *ThreadsafeCornelkMap[K, V]) LoadOrStore(key K, value V) (ret V, loaded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	actual, loaded := m.hashmap.GetOrInsert(key, value)
	if actual != nil {
		ret = actual.(V)
	}
	return ret, loaded
}

func (m *ThreadsafeCornelkMap[K, V]) CompareAndSwap(key K, oldVal V, newVal V) (val V, swapped bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hashmap.Cas(key, oldVal, newVal) {
		return newVal, true
	} else {
		return oldVal, false
	}
}

// CompareAndDelete atomically deletes the value stored at the specified key if that value is equal
// to the provided value. If this is the case, then true is returned. Otherwise, false is returned.
func (m *ThreadsafeCornelkMap[K, V]) CompareAndDelete(key K, oldVal V) (deleted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hashmap.Cas(key, oldVal, deleted) {
		return true
	} else {
		return false
	}
}

func (m *ThreadsafeCornelkMap[K, V]) Range(cb func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	next := true
	for item := range m.hashmap.Iter() {
		if next {
			v, _ := item.Value.(V)
			next = cb(item.Key.(K), v)
		}
		// iterate over all items to drain the channel
	}
}

func (m *ThreadsafeCornelkMap[K, V]) RangeSafe(cb func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	next := true
	for item := range m.hashmap.Iter() {
		if next {
			v, _ := item.Value.(V)
			next = cb(item.Key.(K), v)
		}
		// iterate over all items to drain the channel
	}
}

func (m *ThreadsafeCornelkMap[K, V]) Store(key K, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.hashmap.Set(key, val)
}

func (m *ThreadsafeCornelkMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.hashmap.Len()
}

func (m *ThreadsafeCornelkMap[K, V]) get(key K) (interface{}, bool) {
	if m.stringKey {
		return m.hashmap.GetStringKey(m.assertString(key))
	} else {
		return m.hashmap.Get(key)
	}
}

func (m *ThreadsafeCornelkMap[K, V]) assertString(str interface{}) string {
	return str.(string)
}
