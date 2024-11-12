package scheduling_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestScheduling(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduling Suite")
}
