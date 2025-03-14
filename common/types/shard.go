package types

import "fmt"

func StrFnv32[K fmt.Stringer](key K) uint32 {
	return Fnv32(key.String())
}

func Int32StringerFnv32(key int32) uint32 {
	return Fnv32(fmt.Sprintf("%d", key))
}

func Fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
