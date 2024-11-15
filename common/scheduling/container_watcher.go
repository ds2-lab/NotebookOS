package scheduling

// ContainerWatcher watches for new Pods/Containers.
//
// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
type ContainerWatcher interface {
	// RegisterChannel registers a channel that is used to notify waiting goroutines that the Pod/Container has started.
	//
	// Accepts as a parameter a chan string that can be used to wait until the new Container has been created.
	// The ID of the new Container will be sent over the channel when the new Container is started.
	// The error will be nil on success.
	RegisterChannel(kernelId string, startedChan chan string)
}
