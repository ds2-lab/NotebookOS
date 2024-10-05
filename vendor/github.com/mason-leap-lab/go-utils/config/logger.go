package config

import (
	"reflect"

	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	LogDefault = "default"
)

var (
	DefaultLogLevel = logger.LOG_LEVEL_INFO
	LogLevel        = DefaultLogLevel
	LogColor        = true
	Verbose         bool
)

type LoggerOptions struct {
	Options

	Debug   bool `name:"debug" description:"Display debug logs."`
	Verbose bool `name:"v" description:"Display verbose logs."`
}

func (o *LoggerOptions) Validate() error {
	if o.Debug {
		LogLevel = logger.LOG_LEVEL_ALL
	} else {
		LogLevel = DefaultLogLevel
	}

	Verbose = o.Verbose

	return nil
}

func GetDefaultLogger() logger.Logger {
	return GetLogger(LogDefault)
}

func GetLogger(prefix string) logger.Logger {
	return &logger.ColorLogger{Prefix: prefix, Color: LogColor, Level: LogLevel, Verbose: Verbose}
}

func InitLogger(log *logger.Logger, prefix interface{}) {
	if *log != nil && *log != logger.NilLogger {
		return
	}

	var prfx string
	switch t := prefix.(type) {
	case string:
		prfx = t
	default:
		prfx = reflect.TypeOf(t).Elem().Name() + " "
	}

	ptr := reflect.ValueOf(log)
	ptr.Elem().Set(reflect.ValueOf(GetLogger(prfx)))
}
