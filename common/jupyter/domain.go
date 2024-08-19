package jupyter

import (
	"errors"
	"regexp"
	"time"
)

var (
	DefaultRequestTimeout  = 2 * time.Second
	ZMQDestFrameFormatter  = "dest.%s.req.%s"                                               // dest.<kernel-id>.req.<req-id>
	ZMQDestFrameRecognizer = regexp.MustCompile(`^dest\.([0-9a-z-_]+)\.req\.([0-9a-z-]+)$`) // Changed from a-f to a-z, as IDs can now be arbitrary strings, not just UUIDs.

	ZMQSourceKernelFrameFormatter  = "src.%s"                                   // src.<kernel-id>
	ZMQSourceKernelFrameRecognizer = regexp.MustCompile(`^src\.([0-9a-z-_]+)$`) // Changed from a-f to a-z, as IDs can now be arbitrary strings, not just UUIDs.

	WROptionRemoveDestFrame         = "RemoveDestFrame"
	WROptionRemoveSourceKernelFrame = "RemoveSourceKernelFrame"

	MessageTypeACK = "ACK"

	// Message type sent by our custom Golang Jupyter frontend clients.
	// These inform us that we should expect the frontend to send ACKs, which does not happen for "regular" Jupyter frontends.
	GolangFrontendRegistration = "golang_frontend_registration"

	JOffsetAutoDetect = -1

	ErrNoAck = errors.New("failed to receive ACK for message after maximum number of attempts")
)
