package types

import (
	"github.com/go-zeromq/zmq4"
	"time"
)

func GetSocketOptions() []zmq4.Option {
	return []zmq4.Option{
		zmq4.WithTimeout(time.Millisecond * 5000),
		zmq4.WithAutomaticReconnect(true),
		zmq4.WithDialerMaxRetries(20),
		zmq4.WithDialerRetry(time.Millisecond * 250),
		zmq4.WithDialerTimeout(time.Millisecond * 5000),
	}
}
