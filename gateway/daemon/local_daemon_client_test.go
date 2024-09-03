package daemon

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

var (
	testMetaKey scheduling.HostMetaKey = "test"
)

func dummyScheduler(addr string) (*LocalDaemonClient, error) {
	id := uuid.New().String()
	scheduler := &LocalDaemonClient{
		LocalGatewayClient: proto.NewLocalGatewayClient(nil),
		addr:               addr,
		conn:               nil,
		meta:               hashmap.NewCornelkMap[string, interface{}](10),
	}

	scheduler.id = id
	return scheduler, nil
}

var _ = Describe("Local Daemon Client Tests", func() {
	It("should meta be correctly set and load.", func() {
		scheduler, _ := dummyScheduler("test")
		scheduler.SetMeta(testMetaKey, "test")
		Expect(scheduler.GetMeta(testMetaKey)).To(Equal("test"))
	})
})
