package logger

// Func Function wrapper that support lazy evaluation for the logger
type Func func() string

func (f Func) String() string {
	return f()
}

// NewFunc Create the function wrapper for func() string
func NewFunc(f Func) Func {
	return f
}

// NewFuncWithArg Create the function wrapper for func(arg interface{}) string
func NewFuncWithArg(f func(arg interface{}) string, arg interface{}) Func {
	return func() string {
		return f(arg)
	}
}

// NewFuncWithArgs Create the function wrapper for func(arg ...interface{}) string
func NewFuncWithArgs(f func(arg ...interface{}) string, args ...interface{}) Func {
	return func() string {
		return f(args...)
	}
}

// NewFormatFunc Create the function wrapper that compatible with fmt.Sprintf
func NewFormatFunc(f func(msg string, arg ...interface{}) string, msg string, args ...interface{}) Func {
	return func() string {
		return f(msg, args...)
	}
}
