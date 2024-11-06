package config

import (
	"reflect"

	"github.com/Scusemua/go-utils/logger"
)

const (
	LogDefault = "default"
)

var (
	DefaultLogLevel = logger.LOG_LEVEL_INFO

	// LogLevel is the threshold for this ColorLogger.
	//
	// Logging messages which are less severe than level will be ignored.
	//
	// Logging messages with a severity equal or greater than level will be emitted.
	//
	// Levels are represented numerically, with 0 being the most "severe" and 3 being the least "severe".
	//
	// When LogLevel is set to LOG_LEVEL_NONE (3), no messages will be emitted.
	//
	// When LogLevel is set to LOG_LEVEL_ALL, all messages except for Trace messages will be emitted.
	//
	// In order for Trace messages to be emitted, LogLevel must be set to LOG_LEVEL_ALL, and Verbose must be set to true.
	LogLevel = DefaultLogLevel

	// LogColor is a boolean flag indicating whether colored output is enabled (true) or not (false).
	LogColor = true

	// Verbose must be set to true in order for Trace log messages to be emitted.
	Verbose bool

	// LogTypePrefix is a bool flag that, when true, instructs the logger to also prepend the type of
	// a given log message (e.g., "ERROR", "WARNING", "DEBUG", "TRACE", or "INFO") to the beginning of
	// the message when emitting it.
	LogTypePrefix = true
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
	return &logger.ColorLogger{
		Prefix:        prefix,
		Color:         LogColor,
		Level:         LogLevel,
		Verbose:       Verbose,
		LogTypePrefix: LogTypePrefix,
	}
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
