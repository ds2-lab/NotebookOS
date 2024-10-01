package hashmap

var (
	deleted = &struct{}{}
)

type BaseHashMap[K any, V any] interface {
	Delete(K)
	Load(K) (val V, loaded bool)
	LoadAndDelete(K) (val V, exists bool)
	LoadOrStore(K, V) (val V, loaded bool)
	CompareAndSwap(K, V, V) (val V, swapped bool)

	// Range iterates over the map's key/value pairs, if the callback function returns true, iteration stops.
	Range(func(K, V) (contd bool))

	Store(K, V)
}

type HashMap[K any, V any] interface {
	BaseHashMap[K, V]
	Len() int
}
