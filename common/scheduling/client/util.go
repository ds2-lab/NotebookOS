package client

import "github.com/scusemua/distributed-notebook/common/jupyter/messaging"

func ExtractIOTopicFrame(msg *messaging.JupyterMessage) (string, *messaging.JupyterFrames) {
	if msg.Offset() == 0 {
		return "", nil
	}

	rawTopic := msg.JupyterFrames.Frames[ /*jOffset*/ msg.Offset()-1]
	matches := messaging.IOTopicStatusRecognizer.FindSubmatch(rawTopic)
	if len(matches) > 0 {
		return string(matches[2]), msg.JupyterFrames
	}

	return string(rawTopic), msg.JupyterFrames
}
