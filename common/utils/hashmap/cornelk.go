package hashmap

import (
	"fmt"
	"github.com/zhangjyr/hashmap"
	"log"
	"reflect"
)

// SyncPool using sync.Pool
type CornelkMap[K any, V any] struct {
	hashmap   *hashmap.HashMap
	stringKey bool
}

func NewCornelkMap[K any, V any](size int) *CornelkMap[K, V] {
	var key K
	return &CornelkMap[K, V]{
		stringKey: reflect.TypeOf(key).Kind() == reflect.String,
		hashmap:   hashmap.New((uintptr)(size)),
	}
}

func (m *CornelkMap[K, V]) Delete(key K) {
	m.hashmap.Del(key)
}

func (m *CornelkMap[K, V]) Load(key K) (ret V, ok bool) {
	v, ok := m.get(key)
	if v != nil {
		ret, ok = v.(V)
		if !ok {
			log.Panicf("CornelkMap.Load: type mismatch %v\n", v)
			panic(fmt.Sprintf("CornelkMap.Load: type mismatch %v\n", v))
		}
	}
	return ret, ok
}

func (m *CornelkMap[K, V]) LoadAndDelete(key K) (ret V, retExists bool) {
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

func (m *CornelkMap[K, V]) LoadOrStore(key K, value V) (ret V, loaded bool) {
	actual, loaded := m.hashmap.GetOrInsert(key, value)
	if actual != nil {
		ret = actual.(V)
	}
	return ret, loaded
}

func (m *CornelkMap[K, V]) CompareAndSwap(key K, oldVal V, newVal V) (val V, swapped bool) {
	if m.hashmap.Cas(key, oldVal, newVal) {
		return newVal, true
	} else {
		return oldVal, false
	}
}

func (m *CornelkMap[K, V]) Range(cb func(K, V) bool) {
	next := true
	for item := range m.hashmap.Iter() {
		if next {
			v, _ := item.Value.(V)
			next = cb(item.Key.(K), v)
		}
		// iterate over all items to drain the channel
	}
}

func (m *CornelkMap[K, V]) RangeSafe(cb func(K, V) bool) {
	next := true

	kvs := make([]hashmap.KeyValue, 0, m.hashmap.Len())
	for kv := range m.hashmap.Iter() {
		kvs = append(kvs, kv)
	}

	for i := 0; i < len(kvs); i++ {
		if kvs[i].Value == nil {
			continue
		}

		key := kvs[i].Key.(K)
		val := kvs[i].Value.(V)

		if next {
			next = cb(key, val)
		} else {
			break
		}
	}
}

func (m *CornelkMap[K, V]) Store(key K, val V) {
	m.hashmap.Set(key, val)
}

func (m *CornelkMap[K, V]) Len() int {
	return m.hashmap.Len()
}

func (m *CornelkMap[K, V]) get(key K) (interface{}, bool) {
	if m.stringKey {
		return m.hashmap.GetStringKey(m.assertString(key))
	} else {
		return m.hashmap.Get(key)
	}
}

func (m *CornelkMap[K, V]) assertString(str interface{}) string {
	return str.(string)
}
