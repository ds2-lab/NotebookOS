package logger

import (
	"fmt"
	"log"
	"strings"

	"github.com/mgutz/ansi"
)

// ColorLogger - A Logger that logs to stdout in color
type ColorLogger struct {
	// Verbose must be set to true in order for Trace log messages to be emitted.
	Verbose bool

	// Level is the threshold for this ColorLogger.
	//
	// Logging messages which are less severe than level will be ignored.
	//
	// Logging messages with a severity equal or greater than level will be emitted.
	//
	// Levels are represented numerically, with 0 being the most "severe" and 3 being the least "severe".
	//
	// When Level is set to LOG_LEVEL_NONE (3), no messages will be emitted.
	//
	// When Level is set to LOG_LEVEL_ALL, all messages except for Trace messages will be emitted.
	//
	// In order for Trace messages to be emitted, Level must be set to LOG_LEVEL_ALL, and Verbose must be set to true.
	Level int

	// Prefix is intended to be the name of the entity emitting the log messages.
	// The prefix is prepended to the beginning of every log message.
	Prefix string

	// Color is a boolean flag indicating whether colored output is enabled (true) or not (false).
	Color bool

	// LogTypePrefix is a bool flag that, when true, instructs the logger to also prepend the type of
	// a given log message (e.g., "ERROR", "WARNING", "DEBUG", "TRACE", or "INFO") to the beginning of
	// the message when emitting it.
	LogTypePrefix bool
}

// Trace - Log a very verbose trace message
func (logger *ColorLogger) Trace(format string, args ...interface{}) {
	if !logger.Verbose {
		return
	}
	logger.log(LOG_LEVEL_ALL, "blue", "TRACE", format, args...)
}

// Debug - Log a debug message
func (logger *ColorLogger) Debug(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_ALL, "grey", "DEBUG", format, args...)
}

// Info - Log a general message
func (logger *ColorLogger) Info(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_INFO, "green", "INFO", format, args...)
}

// Warn - Log a warning
func (logger *ColorLogger) Warn(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_WARN, "yellow", "WARN", format, args...)
}

// Error - Log a error
func (logger *ColorLogger) Error(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_NONE, "red", "ERROR", format, args...)
}

// Warn - no-op
func (logger *ColorLogger) GetLevel() int {
	return logger.Level
}

func (logger *ColorLogger) log(threshold int, color, logTypePrefix string, format string, args ...interface{}) {
	if logger.Level > threshold {
		return
	}

	msg := fmt.Sprintf(format, args...)
	if logger.Color && color != "" {
		lines := strings.Split(msg, "\n")
		for i := range lines {
			lines[i] = ansi.Color(lines[i], color)
		}
		msg = strings.Join(lines, "\n")

		logTypePrefix = ansi.Color(logTypePrefix, color)
	}

	log.Println("[" + logTypePrefix + "] " + logger.Prefix + msg)
}
