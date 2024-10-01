package cache

import (
	"errors"
	"reflect"
)

var (
	ErrNotFunction     = errors.New("not function")
	ErrInvalidFunction = errors.New("invalid function")
	DefaultValidKey    = 1
)

// Inline cache is a general variable cache for costy operation.
// Example usage:
// type TypeA struct {
//   variable InlineCache
// }
//
// func NewTypeA() *TypeA {
// 	typeA := &TypeA{}
// 	typeA.variable.Producer = NormalizeChainedICProducer(typeA.costOperation)
// 	return typeA
// }
//
// func (f *Foo) GetVariable(args ...interface{}) TypeB {
// 	return f.variable.Value(args...)
// }
//
// func (f *Foo) costOperation(cached TypeB, args ...interface{}) (ret TypeB) {
// 	// ret = Compute with args...
// 	return
//

// ICProducer defines formal producer: func(cached TypeA, args...) (value TypeA, error)
// Alternation:
// func(cached TypeA, args) (value TypeA)
// func(args) (value TypeA, error)
// func(args) (value TypeA)
type ICProducer func(interface{}, ...interface{}) (interface{}, error)

// ICValidator defines formal validator: func(cached TypeA) (validity bool)
type ICValidator func(interface{}) bool

// TODO: Add building validKey support
// ICValidator defines formal validator: func(cachedValidKey TypeB, cached TypeA) (validity bool, validKey TypeB)
// Alternation:
// func(cached TypeA) (validity bool)
// func() (validity bool)
// type ICValidator func(interface{}, interface{}) (bool, interface{})

type InlineCache struct {
	Producer  ICProducer
	Validator ICValidator

	cached interface{}
	// validKey interface{}
}

func (c *InlineCache) Value(args ...interface{}) interface{} {
	cached, _ := c.ValueWithError(args...)
	return cached
}

func (c *InlineCache) ValueWithError(args ...interface{}) (cached interface{}, err error) {
	if c.cached == nil || (c.Validator != nil && !c.Validator(c.cached)) {
		c.cached, err = c.Producer(c.cached, args...)
	}
	return c.cached, err
}

func (c *InlineCache) Invalidate() {
	c.cached = nil
}

func TryFormalizeICProducer(f interface{}) (ICProducer, error) {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		return nil, ErrNotFunction
	} else if ft.NumOut() < 1 || (ft.NumOut() > 1 && ft.Out(1) != reflect.TypeOf(ErrInvalidFunction)) {
		return nil, ErrInvalidFunction
	}
	return formalizeICProducer(f, 0), nil
}

func FormalizeICProducer(f interface{}) ICProducer {
	return formalizeICProducer(f, 0)
}

func TryFormalizeChainedICProducer(f interface{}) (ICProducer, error) {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		return nil, ErrNotFunction
	} else if ft.NumIn() < 1 || ft.NumOut() < 1 || ft.In(0) != ft.Out(0) ||
		(ft.NumOut() > 1 && ft.Out(1) != reflect.TypeOf(ErrInvalidFunction)) {
		return nil, ErrInvalidFunction
	}
	return formalizeICProducer(f, 1), nil
}

func FormalizeChainedICProducer(f interface{}) ICProducer {
	return formalizeICProducer(f, 1)
}

func formalizeICProducer(f interface{}, chained int) ICProducer {
	fv := reflect.ValueOf(f)
	fargs := make([]reflect.Value, fv.Type().NumIn())
	fargs0 := reflect.Zero(fv.Type().Out(0))
	return func(cached interface{}, args ...interface{}) (interface{}, error) {
		if chained > 0 && cached == nil {
			fargs[0] = fargs0
		} else if chained > 0 {
			fargs[0] = reflect.ValueOf(cached)
		}

		for i := 0; i < len(args); i++ {
			fargs[i+chained] = reflect.ValueOf(args[i])
		}
		rets := fv.Call(fargs)
		if len(rets) < 2 {
			return rets[0].Interface(), nil
		} else {
			return rets[0].Interface(), rets[1].Interface().(error)
		}
	}
}

func TryFormalizeICValidator(f interface{}) (ICValidator, error) {
	ft := reflect.TypeOf(f)
	if ft.Kind() != reflect.Func {
		return nil, ErrNotFunction
	} else if ft.NumIn() != 1 || ft.NumOut() < 1 || ft.Out(0) != reflect.TypeOf(false) {
		return nil, ErrInvalidFunction
	}
	return FormalizeICValidator(f), nil
}

func FormalizeICValidator(f interface{}) ICValidator {
	fv := reflect.ValueOf(f)
	fargs := []reflect.Value{{}}
	return func(cached interface{}) bool {
		fargs[0] = reflect.ValueOf(cached)
		rets := fv.Call(fargs)
		return rets[0].Interface().(bool)
	}
}
