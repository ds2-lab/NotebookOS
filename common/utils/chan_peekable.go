package utils

type ChanPeekable[V any] struct {
	cin     chan V
	cout    chan V
	head    interface{}
	lenHead int
}

func NewChanPeekable[V any](size int) *ChanPeekable[V] {
	if size < 1 {
		size = 1
	}
	ch := &ChanPeekable[V]{
		cin:  make(chan V, size-1),
		cout: make(chan V),
	}
	go ch.serve()
	return ch
}

// Len returns the number of messages in the channel.
func (c *ChanPeekable[V]) Len() int {
	return c.lenHead + len(c.cin)
}

// In returns the input channel.
func (c *ChanPeekable[V]) In() chan<- V {
	return c.cin
}

// Out returns the output channel.
func (c *ChanPeekable[V]) Out() <-chan V {
	return c.cout
}

// Peek returns the head of the channel.
func (c *ChanPeekable[V]) Peek() V {
	var empty V
	head := c.head
	if head == nil {
		return empty
	}
	return head.(V)
}

// Close closes the channel.
func (c *ChanPeekable[V]) Close() {
	close(c.cin)
}

func (c *ChanPeekable[V]) serve() {
	// If there is any message in the input, read it immediately and set it as the head.
	for c.head = range c.cin {
		c.lenHead = 1
		// The channel blocks if there is no reader.
		c.cout <- c.head.(V)
		// Reset head to nil before read another message.
		c.head = nil
		c.lenHead = 0
	}
	// No more, close the output channel.
	close(c.cout)
}
