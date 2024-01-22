package daemon

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

var (
	testMetaKey core.HostMetaKey = "test"
)

func dummyScheduler(addr string) (*HostScheduler, error) {
	id := uuid.New().String()
	scheduler := &HostScheduler{
		LocalGatewayClient: gateway.NewLocalGatewayClient(nil),
		addr:               addr,
		conn:               nil,
		meta:               hashmap.NewCornelkMap[string, interface{}](10),
	}

	scheduler.id = id
	return scheduler, nil
}

var _ = Describe("HostScheduler", func() {
	It("should meta be correctly set and load.", func() {
		scheduler, _ := dummyScheduler("test")
		scheduler.SetMeta(testMetaKey, "test")
		Expect(scheduler.GetMeta(testMetaKey)).To(Equal("test"))
	})
})
