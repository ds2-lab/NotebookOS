package placer_test

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPlacer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Placer Suite")
}

// Un-comment this to enable debug logging for all test files in the suite.
var _ = BeforeSuite(func() {
	config.LogLevel = logger.LOG_LEVEL_ALL
})
