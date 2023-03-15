package server

import (
	"fmt"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func genKernelFrame(kernelID string, ids ...string) []byte {
	reqId := uuid.New().String()
	if len(ids) > 0 {
		reqId = ids[0]
	}

	return []byte(fmt.Sprintf(ZMQDestFrameFormatter, kernelID, reqId))
}

var _ = Describe("BaseServer", func() {
	var srv *BaseServer

	setup := func() {
		srv = &BaseServer{}
	}

	It("should AddDestFrame add kernel frame just before <IDS|MSG>.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		added, _ := srv.AddDestFrame(frames, "d5d29f07-bbdb-485a-a98c-1a1a5e21b824", JOffsetAutoDetect)
		Expect(len(added)).To(Equal(2))
		match := ZMQDestFrameRecognizer.FindStringSubmatch(string(added[0]))
		Expect(len(match)).To(Equal(3))
		Expect(match[1]).To(Equal("d5d29f07-bbdb-485a-a98c-1a1a5e21b824"))
		Expect(string(added[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		added, _ = srv.AddDestFrame(frames, "d5d29f07-bbdb-485a-a98c-1a1a5e21b824", JOffsetAutoDetect)
		Expect(len(added)).To(Equal(4))
		Expect(added[0]).To(Equal(frames[0]))
		match = ZMQDestFrameRecognizer.FindStringSubmatch(string(added[1]))
		Expect(len(match)).To(Equal(3))
		Expect(match[1]).To(Equal("d5d29f07-bbdb-485a-a98c-1a1a5e21b824"))
		Expect(string(added[2])).To(Equal("<IDS|MSG>"))
		Expect(string(added[3])).To(Equal("body"))
	})

	It("should ExtractDestFrame extract kernel id and jupyter frames.", func() {
		setup()

		frames := [][]byte{
			[]byte("some identities"),
			genKernelFrame("d5d29f07-bbdb-485a-a98c-1a1a5e21b824", "a98c"),
			[]byte("<IDS|MSG>"),
		}

		kernelId, reqId, offset := srv.ExtractDestFrame(frames)
		Expect(kernelId).To(Equal("d5d29f07-bbdb-485a-a98c-1a1a5e21b824"))
		Expect(reqId).To(Equal("a98c"))
		Expect(offset).To(Equal(2))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
		}
		kernelId, _, offset = srv.ExtractDestFrame(frames)
		Expect(kernelId).To(Equal(""))
		Expect(offset).To(Equal(1))

		frames = [][]byte{
			[]byte("<IDS|MSG>"),
		}
		kernelId, _, offset = srv.ExtractDestFrame(frames)
		Expect(kernelId).To(Equal(""))
		Expect(offset).To(Equal(0))
	})

	It("should RemoveDestFrame remove kernel frame.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		removed := srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(removed)).To(Equal(1))
		Expect(string(removed[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			genKernelFrame("d5d29f07-bbdb-485a-a98c-1a1a5e21b824", "a98c"),
			[]byte("<IDS|MSG>"),
		}
		removed = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(removed)).To(Equal(1))
		Expect(string(removed[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			genKernelFrame("d5d29f07-bbdb-485a-a98c-1a1a5e21b824"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		removed = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(removed)).To(Equal(3))
		Expect(string(removed[0])).To(Equal("some identities"))
		Expect(string(removed[1])).To(Equal("<IDS|MSG>"))
		Expect(string(removed[2])).To(Equal("body"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
		}
		removed = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(removed)).To(Equal(2))
		Expect(string(removed[0])).To(Equal("some identities"))
		Expect(string(removed[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("3c6669b1-5208-42a1-bf66-4af54cc9000b"),
			genKernelFrame("d5d29f07-bbdb-485a-a98c-1a1a5e21b824"),
			[]byte("<IDS|MSG>"),
			[]byte("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"),
		}
		removed = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(removed)).To(Equal(3))
		Expect(string(removed[0])).To(Equal("3c6669b1-5208-42a1-bf66-4af54cc9000b"))
		Expect(string(removed[1])).To(Equal("<IDS|MSG>"))
		Expect(string(removed[2])).To(Equal("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"))
	})
})
