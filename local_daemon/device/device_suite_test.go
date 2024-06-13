package device_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDevice(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Device Suite")
}
