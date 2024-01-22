package server

import (
	"fmt"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	SOURCE_KERNEL_ID = "00a9c7f9-6406-464e-a000-f1bf9e67c830"
	DEST_KERNEL_ID   = "d5d29f07-bbdb-485a-a98c-1a1a5e21b824"
	EXTRA_KERNEL_ID  = "3c6669b1-5208-42a1-bf66-4af54cc9000b"
)

func getDestFrame(kernelID string, ids ...string) []byte {
	reqId := uuid.New().String()
	if len(ids) > 0 {
		reqId = ids[0]
	}

	return []byte(fmt.Sprintf(ZMQDestFrameFormatter, kernelID, reqId))
}

func getSourceKernelFrame(kernelID string) []byte {
	return []byte(fmt.Sprintf(ZMQSourceKernelFrameFormatter, kernelID))
}

func printFrames(frames [][]byte) {
	for i, frame := range frames {
		GinkgoWriter.Printf("Frame #%d: \"%s\"\n", i, frame)
	}
}

var _ = Describe("BaseServer", func() {
	var srv *BaseServer

	setup := func() {
		srv = &BaseServer{}
	}

	It("should use AddSourceKernelFrame to add a SourceKernelFrame just before the DestFrame when there is already a DestFrame.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		frames, _ = srv.AddDestFrame(frames, DEST_KERNEL_ID, JOffsetAutoDetect)
		added := srv.AddSourceKernelFrame(frames, SOURCE_KERNEL_ID, JOffsetAutoDetect)
		printFrames(added)
		Expect(len(added)).To(Equal(3))
		match := ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(added[0]))
		GinkgoWriter.Printf("match: %v\n", match)
		Expect(len(match)).To(Equal(2))
		Expect(match[1]).To(Equal(SOURCE_KERNEL_ID))
		Expect(string(added[2])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		frames, _ = srv.AddDestFrame(frames, DEST_KERNEL_ID, JOffsetAutoDetect)
		added = srv.AddSourceKernelFrame(frames, SOURCE_KERNEL_ID, JOffsetAutoDetect)
		printFrames(added)
		Expect(len(added)).To(Equal(5))
		Expect(added[0]).To(Equal(frames[0]))
		match = ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(added[1]))
		GinkgoWriter.Printf("match: %v\n", match)
		Expect(len(match)).To(Equal(2))
		Expect(match[1]).To(Equal(SOURCE_KERNEL_ID))
		Expect(string(added[3])).To(Equal("<IDS|MSG>"))
		Expect(string(added[4])).To(Equal("body"))
	})

	It("should use AddSourceKernelFrame to add a SourceKernelFrame just before <IDS|MSG> when there is no DestFrame already present.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		added := srv.AddSourceKernelFrame(frames, SOURCE_KERNEL_ID, JOffsetAutoDetect)
		Expect(len(added)).To(Equal(2))
		match := ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(added[0]))
		printFrames(added)
		GinkgoWriter.Printf("match: %v\n", match)
		Expect(len(match)).To(Equal(2))
		Expect(match[1]).To(Equal(SOURCE_KERNEL_ID))
		Expect(string(added[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		added = srv.AddSourceKernelFrame(frames, SOURCE_KERNEL_ID, JOffsetAutoDetect)
		Expect(len(added)).To(Equal(4))
		Expect(added[0]).To(Equal(frames[0]))
		match = ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(added[1]))
		printFrames(added)
		GinkgoWriter.Printf("match: %v\n", match)
		Expect(len(match)).To(Equal(2))
		Expect(match[1]).To(Equal(SOURCE_KERNEL_ID))
		Expect(string(added[2])).To(Equal("<IDS|MSG>"))
		Expect(string(added[3])).To(Equal("body"))
	})

	It("should AddDestFrame to add a kernel frame just before <IDS|MSG>.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		added, _ := srv.AddDestFrame(frames, DEST_KERNEL_ID, JOffsetAutoDetect)
		Expect(len(added)).To(Equal(2))
		match := ZMQDestFrameRecognizer.FindStringSubmatch(string(added[0]))
		printFrames(added)
		GinkgoWriter.Printf("match: %v\n\n", match)
		Expect(len(match)).To(Equal(3))
		Expect(match[1]).To(Equal(DEST_KERNEL_ID))
		Expect(string(added[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		added, _ = srv.AddDestFrame(frames, DEST_KERNEL_ID, JOffsetAutoDetect)
		Expect(len(added)).To(Equal(4))
		Expect(added[0]).To(Equal(frames[0]))
		match = ZMQDestFrameRecognizer.FindStringSubmatch(string(added[1]))
		printFrames(added)
		GinkgoWriter.Printf("match: %v\n\n", match)
		Expect(len(match)).To(Equal(3))
		Expect(match[1]).To(Equal(DEST_KERNEL_ID))
		Expect(string(added[2])).To(Equal("<IDS|MSG>"))
		Expect(string(added[3])).To(Equal("body"))

		frames = [][]byte{
			[]byte("some identities"),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		added, _ = srv.AddDestFrame(frames, DEST_KERNEL_ID, JOffsetAutoDetect)
		Expect(len(added)).To(Equal(5))
		Expect(added[0]).To(Equal(frames[0]))
		Expect(added[1]).To(Equal(frames[1]))
		match = ZMQDestFrameRecognizer.FindStringSubmatch(string(added[2]))
		printFrames(added)
		GinkgoWriter.Printf("match: %v\n\n", match)
		Expect(len(match)).To(Equal(3))
		Expect(match[1]).To(Equal(DEST_KERNEL_ID))
		Expect(string(added[3])).To(Equal("<IDS|MSG>"))
		Expect(string(added[4])).To(Equal("body"))
	})

	It("should use ExtractDestFrame to extract kernel id and jupyter frames.", func() {
		setup()

		frames := [][]byte{
			[]byte("some identities"),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}

		kernelId, reqId, offset := srv.ExtractDestFrame(frames)
		Expect(kernelId).To(Equal(DEST_KERNEL_ID))
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

	It("should use ExtractSourceKernelFrame to extract kernel id.", func() {
		setup()

		frames := [][]byte{
			[]byte("some identities"),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			[]byte("<IDS|MSG>"),
		}

		kernelId, offset := srv.ExtractSourceKernelFrame(frames)
		printFrames(frames)
		GinkgoWriter.Printf("\nKernel ID: \"%s\", Offset: %d.\n\n\n", kernelId, offset)
		Expect(kernelId).To(Equal(SOURCE_KERNEL_ID))
		Expect(offset).To(Equal(2))

		frames = [][]byte{
			[]byte("some identities"),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}

		kernelId, offset = srv.ExtractSourceKernelFrame(frames)
		printFrames(frames)
		GinkgoWriter.Printf("\nKernel ID: \"%s\", Offset: %d.\n\n\n", kernelId, offset)
		Expect(kernelId).To(Equal(SOURCE_KERNEL_ID))
		Expect(offset).To(Equal(2))

		frames = [][]byte{
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}

		kernelId, offset = srv.ExtractSourceKernelFrame(frames)
		printFrames(frames)
		GinkgoWriter.Printf("\nKernel ID: \"%s\", Offset: %d.\n\n\n", kernelId, offset)
		Expect(kernelId).To(Equal(SOURCE_KERNEL_ID))
		Expect(offset).To(Equal(1))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
		}
		kernelId, offset = srv.ExtractSourceKernelFrame(frames)
		printFrames(frames)
		GinkgoWriter.Printf("\nKernel ID: \"%s\", Offset: %d.\n\n\n", kernelId, offset)
		Expect(kernelId).To(Equal(""))
		Expect(offset).To(Equal(1))

		frames = [][]byte{
			[]byte("<IDS|MSG>"),
		}
		kernelId, offset = srv.ExtractSourceKernelFrame(frames)
		printFrames(frames)
		GinkgoWriter.Printf("\nKernel ID: \"%s\", Offset: %d.\n\n\n", kernelId, offset)
		Expect(kernelId).To(Equal(""))
		Expect(offset).To(Equal(0))
	})

	It("should use RemoveDestFrame to remove a kernel frame.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		frames_after_removal := srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(1))
		Expect(string(frames_after_removal[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(1))
		Expect(string(frames_after_removal[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			getDestFrame(DEST_KERNEL_ID),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		frames_after_removal = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(3))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[2])).To(Equal("body"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(2))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte(EXTRA_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID),
			[]byte("<IDS|MSG>"),
			[]byte("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"),
		}
		frames_after_removal = srv.RemoveDestFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(3))
		Expect(string(frames_after_removal[0])).To(Equal(EXTRA_KERNEL_ID))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[2])).To(Equal("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"))
	})

	It("should use RemoveSourceKernelFrame to remove the \"source kernel\" frame.", func() {
		setup()

		frames := [][]byte{
			[]byte("<IDS|MSG>"),
		}

		frames_after_removal := srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(1))
		Expect(string(frames_after_removal[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(1))
		Expect(string(frames_after_removal[0])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(3))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[2])).To(Equal("body"))

		frames = [][]byte{
			[]byte("some identities"),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(2))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte(EXTRA_KERNEL_ID),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			[]byte("<IDS|MSG>"),
			[]byte("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(3))
		Expect(string(frames_after_removal[0])).To(Equal(EXTRA_KERNEL_ID))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[2])).To(Equal("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"))

		frames = [][]byte{
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}

		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(2))
		Expect(string(frames_after_removal[0])).To(Equal(string(getDestFrame(DEST_KERNEL_ID, "a98c"))))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(2))
		Expect(string(frames_after_removal[0])).To(Equal(string(getDestFrame(DEST_KERNEL_ID, "a98c"))))
		Expect(string(frames_after_removal[1])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte("some identities"),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
			[]byte("body"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(4))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal(string(getDestFrame(DEST_KERNEL_ID, "a98c"))))
		Expect(string(frames_after_removal[2])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[3])).To(Equal("body"))

		frames = [][]byte{
			[]byte("some identities"),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(3))
		Expect(string(frames_after_removal[0])).To(Equal("some identities"))
		Expect(string(frames_after_removal[1])).To(Equal(string(getDestFrame(DEST_KERNEL_ID, "a98c"))))
		Expect(string(frames_after_removal[2])).To(Equal("<IDS|MSG>"))

		frames = [][]byte{
			[]byte(EXTRA_KERNEL_ID),
			getSourceKernelFrame(SOURCE_KERNEL_ID),
			getDestFrame(DEST_KERNEL_ID, "a98c"),
			[]byte("<IDS|MSG>"),
			[]byte("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"),
		}
		frames_after_removal = srv.RemoveSourceKernelFrame(frames, JOffsetAutoDetect)
		Expect(len(frames_after_removal)).To(Equal(4))
		Expect(string(frames_after_removal[0])).To(Equal(EXTRA_KERNEL_ID))
		Expect(string(frames_after_removal[1])).To(Equal(string(getDestFrame(DEST_KERNEL_ID, "a98c"))))
		Expect(string(frames_after_removal[2])).To(Equal("<IDS|MSG>"))
		Expect(string(frames_after_removal[3])).To(Equal("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"))
	})
})
