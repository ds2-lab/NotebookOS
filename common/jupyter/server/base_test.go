package server

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
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

	return []byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, kernelID, reqId))
}

func getSourceKernelFrame(kernelID string) []byte {
	return []byte(fmt.Sprintf(jupyter.ZMQSourceKernelFrameFormatter, kernelID))
}

func printFrames(frames [][]byte) {
	for i, frame := range frames {
		GinkgoWriter.Printf("Frame #%d: \"%s\"\n", i, frame)
	}
}

var _ = Describe("BaseServer", func() {
	Context("DestFrames", func() {
		It("should AddDestFrame to add a kernel frame just before <IDS|MSG>.", func() {
			frames := [][]byte{
				[]byte("<IDS|MSG>"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(frames)

			jFrames.AddDestFrame(DEST_KERNEL_ID, true)
			Expect(jFrames.Len()).To(Equal(2))
			match := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(jFrames.Frames[0]))
			printFrames(jFrames.Frames)
			GinkgoWriter.Printf("match: %v\n\n", match)
			Expect(len(match)).To(Equal(3))
			Expect(match[1]).To(Equal(DEST_KERNEL_ID))
			Expect(string(jFrames.Frames[1])).To(Equal("<IDS|MSG>"))

			frames = [][]byte{
				[]byte("some identities"),
				[]byte("<IDS|MSG>"),
				[]byte("body"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)
			jFrames.AddDestFrame(DEST_KERNEL_ID, true)
			Expect(jFrames.Len()).To(Equal(4))
			Expect(jFrames.Frames[0]).To(Equal(frames[0]))
			match = jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(jFrames.Frames[1]))
			printFrames(jFrames.Frames)
			GinkgoWriter.Printf("match: %v\n\n", match)
			Expect(len(match)).To(Equal(3))
			Expect(match[1]).To(Equal(DEST_KERNEL_ID))
			Expect(string(jFrames.Frames[2])).To(Equal("<IDS|MSG>"))
			Expect(string(jFrames.Frames[3])).To(Equal("body"))

			frames = [][]byte{
				[]byte("some identities"),
				getSourceKernelFrame(SOURCE_KERNEL_ID),
				[]byte("<IDS|MSG>"),
				[]byte("body"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)
			jFrames.AddDestFrame(DEST_KERNEL_ID, true)
			Expect(jFrames.Len()).To(Equal(5))
			Expect(jFrames.Frames[0]).To(Equal(frames[0]))
			Expect(jFrames.Frames[1]).To(Equal(frames[1]))
			match = jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(jFrames.Frames[2]))
			printFrames(jFrames.Frames)
			GinkgoWriter.Printf("match: %v\n\n", match)
			Expect(len(match)).To(Equal(3))
			Expect(match[1]).To(Equal(DEST_KERNEL_ID))
			Expect(string(jFrames.Frames[3])).To(Equal("<IDS|MSG>"))
			Expect(string(jFrames.Frames[4])).To(Equal("body"))
		})

		It("should use ExtractDestFrame to extract kernel id and jupyter frames.", func() {
			frames := [][]byte{
				[]byte("some identities"),
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(frames)

			kernelId, reqId, offset := jFrames.ExtractDestFrame(false)
			Expect(kernelId).To(Equal(DEST_KERNEL_ID))
			Expect(reqId).To(Equal("a98c"))
			Expect(offset).To(Equal(2))

			frames = [][]byte{
				[]byte("some identities"),
				[]byte("<IDS|MSG>"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)
			kernelId, _, offset = jFrames.ExtractDestFrame(false)
			Expect(kernelId).To(Equal(""))
			Expect(offset).To(Equal(1))

			frames = [][]byte{
				[]byte("<IDS|MSG>"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)
			kernelId, _, offset = jFrames.ExtractDestFrame(false)
			Expect(kernelId).To(Equal(""))
			Expect(offset).To(Equal(0))
		})

		It("should use RemoveDestFrame to remove a kernel frame.", func() {
			frames := [][]byte{
				[]byte("<IDS|MSG>"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(frames)

			framesAfterRemoval := jFrames.RemoveDestFrame(true)
			Expect(len(framesAfterRemoval)).To(Equal(1))
			Expect(string(framesAfterRemoval[0])).To(Equal("<IDS|MSG>"))

			frames = [][]byte{
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)

			framesAfterRemoval = jFrames.RemoveDestFrame(true)
			Expect(len(framesAfterRemoval)).To(Equal(1))
			Expect(string(framesAfterRemoval[0])).To(Equal("<IDS|MSG>"))

			frames = [][]byte{
				[]byte("some identities"),
				getDestFrame(DEST_KERNEL_ID),
				[]byte("<IDS|MSG>"),
				[]byte("body"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)

			framesAfterRemoval = jFrames.RemoveDestFrame(true)
			Expect(len(framesAfterRemoval)).To(Equal(3))
			Expect(string(framesAfterRemoval[0])).To(Equal("some identities"))
			Expect(string(framesAfterRemoval[1])).To(Equal("<IDS|MSG>"))
			Expect(string(framesAfterRemoval[2])).To(Equal("body"))

			frames = [][]byte{
				[]byte("some identities"),
				[]byte("<IDS|MSG>"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)

			printFrames(jFrames.Frames)
			framesAfterRemoval = jFrames.RemoveDestFrame(true)
			printFrames(jFrames.Frames)

			Expect(len(framesAfterRemoval)).To(Equal(2))
			Expect(string(framesAfterRemoval[0])).To(Equal("some identities"))
			Expect(string(framesAfterRemoval[1])).To(Equal("<IDS|MSG>"))

			frames = [][]byte{
				[]byte(EXTRA_KERNEL_ID),
				getDestFrame(DEST_KERNEL_ID),
				[]byte("<IDS|MSG>"),
				[]byte("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"),
			}
			jFrames = messaging.NewJupyterFramesFromBytes(frames)

			framesAfterRemoval = jFrames.RemoveDestFrame(true)
			Expect(len(framesAfterRemoval)).To(Equal(3))
			Expect(string(framesAfterRemoval[0])).To(Equal(EXTRA_KERNEL_ID))
			Expect(string(framesAfterRemoval[1])).To(Equal("<IDS|MSG>"))
			Expect(string(framesAfterRemoval[2])).To(Equal("adc6e220ddc8d4184576e72f8ca96bca363ecdeab43b136a7917e93afc6bc5e0"))
		})
	})
})
