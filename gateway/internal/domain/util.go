package domain

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
)

type KernelProvider interface {
	GetKernel(kernelId string) (scheduling.Kernel, bool)
}

// KernelAndTypeFromMsg extracts the kernel ID and the message type from the given ZMQ message.
func KernelAndTypeFromMsg(msg *messaging.JupyterMessage, kernelProvider KernelProvider) (kernel scheduling.Kernel, messageType string, err error) {
	// This is initially the kernel's ID, which is the DestID field of the message.
	// But we may not have set a destination ID field within the message yet.
	// In this case, we'll fall back to the session ID within the message's Jupyter header.
	// This may not work either, though, if that session has not been bound to the kernel yet.
	//
	// When Jupyter clients connect for the first time, they send both a shell and a control "kernel_info_request" message.
	// This message is used to bind the session to the kernel (specifically the shell message).
	var kernelId = msg.DestinationId

	// If there is no destination ID, then we'll try to use the session ID in the message's header instead.
	if len(kernelId) == 0 {
		kernelId = msg.JupyterSession()

		// Sanity check.
		// Make sure we got a valid session ID out of the Jupyter message header.
		// If we didn't, then we'll return an error.
		if len(kernelId) == 0 {
			err = fmt.Errorf("%w: message did not contain a destination ID, and session ID was invalid (i.e., the empty string)",
				types.ErrKernelNotFound)
			return nil, msg.JupyterMessageType(), err
		}
	}

	kernel, ok := kernelProvider.GetKernel(kernelId) // kernelId)
	if !ok {
		return nil, messageType, types.ErrKernelNotFound
	}

	return kernel, messageType, nil
}
