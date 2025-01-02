package hashmap_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHashmap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Hashmap Suite")
}
