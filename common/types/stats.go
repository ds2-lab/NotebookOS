package types

type StatIntField interface {
	LoadInt() int
}

type StatInt32Field interface {
	Add(int32) int32
	Sub(int32) int32
	Store(int32)
	Load() int32
}

type StatInt64Field interface {
	Add(int64) int64
	Sub(int64) int64
	Store(int64)
	Load() int64
}

type StatFloat64Field interface {
	Add(float64) float64
	Sub(float64) float64
	Store(float64)
	Load() float64
}
