package types

import (
	"errors"
	uatomic "go.uber.org/atomic"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	ErrInitialized         = errors.New("initialized")
	ErrInitWithNonPtr      = errors.New("can not init using value except pointer")
	ErrInitWithOtherObject = errors.New("can not init using other objects except self")
)

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

type Naming interface {
	Init(Naming) error
	Value(string) interface{}
}

type NamePath struct {
	me reflect.Value
}

func (np *NamePath) Init(me Naming) error {
	if np.me.IsValid() {
		return ErrInitialized
	}

	v := reflect.ValueOf(me)
	if v.Kind() != reflect.Ptr {
		return ErrInitWithNonPtr
	}
	v = v.Elem()
	if vnp := v.FieldByName("NamePath"); !vnp.IsValid() {
		return ErrInitWithOtherObject
	} else if vnp.Interface() != np && (!vnp.CanAddr() || vnp.Addr().Interface() != np) {
		return ErrInitWithOtherObject
	}

	np.me = v
	numField := np.me.NumField()
	for i := 0; i < numField; i++ {
		field := np.me.Field(i)
		if !field.CanInterface() || field.Interface() == np || field.Addr().Interface() == np {
			continue
		}

		var err error
		if child, ok := field.Addr().Interface().(Naming); ok {
			err = child.Init(child)
		} else if child, ok := field.Interface().(Naming); ok && !field.IsNil() {
			err = child.Init(child)
		}
		if err != nil && !errors.Is(err, ErrInitialized) {
			return err
		}
	}
	return nil
}

func (np *NamePath) Value(keyPath string) interface{} {
	if !np.me.IsValid() {
		return nil
	}

	segments := strings.SplitN(keyPath, ".", 2)
	v := np.me.FieldByName(segments[0])
	if !v.IsValid() {
		return nil
	} else if len(segments) < 2 {
		return v.Interface()
	} else if naming, ok := v.Addr().Interface().(Naming); ok {
		return naming.Value(segments[1])
	} else if naming, ok := v.Interface().(Naming); ok && !v.IsNil() {
		return naming.Value(segments[1])
	} else {
		return nil
	}
}

type Stats interface {
	Naming

	IntStats(string) StatIntField
	Int32Stats(string) StatInt32Field
	Int64Stats(string) StatInt64Field
	Float64Stats(string) StatFloat64Field
}

type BaseStats struct {
	NamePath
}

func (s *BaseStats) IntStats(name string) StatIntField {
	switch v := s.Value(name).(type) {
	case int:
		ret := StatInt64(v)
		return &ret
	case int32:
		ret := StatInt32(v)
		return &ret
	case int64:
		ret := StatInt64(v)
		return &ret
	default:
		return v.(StatIntField)
	}
}

func (s *BaseStats) Int32Stats(name string) StatInt32Field {
	switch v := s.Value(name).(type) {
	case int32:
		ret := StatInt32(v)
		return &ret
	default:
		return v.(StatInt32Field)
	}
}

func (s *BaseStats) Int64Stats(name string) StatInt64Field {
	switch v := s.Value(name).(type) {
	case int64:
		ret := StatInt64(v)
		return &ret
	default:
		return v.(StatInt64Field)
	}
}

func (s *BaseStats) Float64Stats(name string) StatFloat64Field {
	return s.Value(name).(StatFloat64Field)
}

type StatInt32 int32

func (i *StatInt32) Add(j int32) int32 {
	return atomic.AddInt32((*int32)(i), j)
}

func (i *StatInt32) Incr() int32 {
	return atomic.AddInt32((*int32)(i), 1)
}

func (i *StatInt32) Decr() int32 {
	return atomic.AddInt32((*int32)(i), -1)
}

func (i *StatInt32) Sub(j int32) int32 {
	return atomic.AddInt32((*int32)(i), ^(j - 1))
}

func (i *StatInt32) Store(j int32) {
	atomic.StoreInt32((*int32)(i), j)
}

func (i *StatInt32) Load() int32 {
	return atomic.LoadInt32((*int32)(i))
}

func (i *StatInt32) LoadInt() int {
	return int(atomic.LoadInt32((*int32)(i)))
}

func (i *StatInt32) String() string {
	return strconv.Itoa(i.LoadInt())
}

type StatInt64 int64

func (i *StatInt64) Add(j int64) int64 {
	return atomic.AddInt64((*int64)(i), j)
}

func (i *StatInt64) Sub(j int64) int64 {
	return atomic.AddInt64((*int64)(i), ^(j - 1))
}

func (i *StatInt64) Store(j int64) {
	atomic.StoreInt64((*int64)(i), j)
}

func (i *StatInt64) Load() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *StatInt64) LoadInt() int {
	return int(atomic.LoadInt64((*int64)(i)))
}

func (i *StatInt64) String() string {
	return strconv.FormatInt(i.Load(), 10)
}

type StatFloat64 struct {
	uatomic.Float64
}

func (f *StatFloat64) LoadInt() int {
	return int(f.Load())
}

type MovingStat struct {
	values    []float64
	sum       [2]float64 // include a moving sum(0) and a reset(1) used to ensuring moving sum by adding only.
	window    int64
	n         int64
	last      int64
	active    int
	resetting int
}

func NewMovingStat(window int64, n int64, values []float64, last int64, sum [2]float64, active int, resetting int) *MovingStat {
	return &MovingStat{window: window, n: n, values: values, last: last, sum: sum, active: active, resetting: resetting}
}

func NewMovingStatFromWindow(window int64) *MovingStat {
	return &MovingStat{
		window:    window,
		n:         0,
		values:    make([]float64, window),
		last:      0,
		active:    0,
		resetting: 1,
	}
}

func (s *MovingStat) Add(val float64) {
	// Move forward.
	s.last = (s.last + 1) % s.window

	// Add difference to sum.
	s.sum[s.active] += val - s.values[s.last]
	s.sum[s.resetting] += val // Resetting is used to sum from ground above each window interval.
	if s.last == 0 {
		s.active = s.resetting
		s.resetting = (s.resetting + 1) % len(s.sum)
		s.sum[s.resetting] = 0.0
	}

	// Record history value
	s.values[s.last] = val

	// update length
	if s.n < s.window {
		s.n += 1
	}
}

func (s *MovingStat) Sum() float64 {
	return s.sum[s.active]
}

func (s *MovingStat) Window() int64 {
	return s.window
}

func (s *MovingStat) N() int64 {
	return s.n
}

func (s *MovingStat) Avg() float64 {
	return s.sum[s.active] / float64(s.n)
}

func (s *MovingStat) Last() float64 {
	return s.values[s.last]
}

func (s *MovingStat) LastN(n int64) float64 {
	if n > s.n {
		n = s.n
	}
	return s.values[(s.last+s.window-n)%s.window]
}
