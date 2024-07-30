package types

// RequestDestination is an interface for describing the destination of a request.
type RequestDest interface {
	// ID returns the ID of the destination.
	RequestDestID() string

	// ExtractDestFrame extracts the destination info from the specified zmq4 frames.
	// Returns the destination ID, request ID and the offset to the jupyter frames.
	ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int)

	// AddDestFrame adds the destination frame to the specified zmq4 frames,
	// which should generate a unique request ID that can be extracted by ExtractDestFrame.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	AddDestFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte, reqID string)

	// RemoveDestFrame removes the destination frame from the specified zmq4 frames.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	RemoveDestFrame(frames [][]byte, jOffset int) (oldFrams [][]byte)
}

// SourceKernel is an interface for describing the kernel from which a particular IO message originated.
// This interface is designed similarly to the RequestDest interface.
type SourceKernel interface {
	// ID returns the ID of the destination.
	SourceKernelID() string

	// ExtractSourceKernelFrame extracts the source kernel info from the specified zmq4 frames.
	// Returns the destination ID, request ID and the offset to the jupyter frames.
	ExtractSourceKernelFrame(frames [][]byte) (destID string, jOffset int)

	// AddSourceKernelFrame adds the source kernel to the specified zmq4 frames,
	// which should generate a unique request ID that can be extracted by ExtractSourceKernelFrame.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	AddSourceKernelFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte)

	// RemoveSourceKernelFrame removes the source kernel frame from the specified zmq4 frames.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	RemoveSourceKernelFrame(frames [][]byte, jOffset int) (oldFrams [][]byte)

	ConnectionInfo() *types.ConnectionInfo
}
