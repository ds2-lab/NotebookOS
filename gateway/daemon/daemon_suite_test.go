package daemon_test

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	debugLoggingEnabled = false
)

func init() {
	if os.Getenv("DEBUG") != "" || os.Getenv("VERBOSE") != "" {
		debugLoggingEnabled = true
	}
}

func TestDaemon(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gateway Daemon Suite")
}

var _ = BeforeSuite(func() {
	if debugLoggingEnabled {
		config.LogLevel = logger.LOG_LEVEL_ALL
	}
})
