package logger

import (
	"fmt"
	"log"
	"strings"

	"github.com/mgutz/ansi"
)

// ColorLogger - A Logger that logs to stdout in color
type ColorLogger struct {
	Verbose bool
	Level   int
	Prefix  string
	Color   bool
}

// Trace - Log a very verbose trace message
func (logger *ColorLogger) Trace(format string, args ...interface{}) {
	if !logger.Verbose {
		return
	}
	logger.log(LOG_LEVEL_ALL, "blue", format, args...)
}

// Debug - Log a debug message
func (logger *ColorLogger) Debug(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_ALL, "grey", format, args...)
}

// Info - Log a general message
func (logger *ColorLogger) Info(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_INFO, "green", format, args...)
}

// Warn - Log a warning
func (logger *ColorLogger) Warn(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_WARN, "yellow", format, args...)
}

// Error - Log a error
func (logger *ColorLogger) Error(format string, args ...interface{}) {
	logger.log(LOG_LEVEL_NONE, "red", format, args...)
}

// Warn - no-op
func (logger *ColorLogger) GetLevel() int {
	return logger.Level
}

func (logger *ColorLogger) log(threshold int, color, format string, args ...interface{}) {
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
	}

	log.Println(logger.Prefix + msg)
}
