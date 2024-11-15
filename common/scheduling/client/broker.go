package client

import (
	"errors"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"reflect"
	"sync"
)

const (
	MessageBrokerAllTopics = "all"
)

type MessageTopicRecognizer[R any, T any] func(msg R) (string, T)

// MessageBroker is a general message broker that offers simple event handling.
// Users will need to provide the type of message source(S), raw message(R), normalized message(T), and a topic recognizer.
type MessageBroker[S any, R any, T any] struct {
	topics            map[string][]scheduling.MessageBrokerHandler[S, T, R]
	topicMutex        sync.RWMutex
	topicRecognizer   MessageTopicRecognizer[R, T]
	allTopicsHandlers []scheduling.MessageBrokerHandler[S, T, R]
}

func NewMessageBroker[S any, R any, T any](recognizer MessageTopicRecognizer[R, T]) *MessageBroker[S, R, T] {
	return &MessageBroker[S, R, T]{
		topics:          make(map[string][]scheduling.MessageBrokerHandler[S, T, R]),
		topicRecognizer: recognizer,
	}
}

func (broker *MessageBroker[S, R, T]) Publish(src S, raw R) (err error) {
	topic, msg := broker.topicRecognizer(raw)
	err = jupyter.ErrNoHandler
	stop := false

	broker.topicMutex.RLock()
	defer broker.topicMutex.RUnlock()
	if handlers, ok := broker.topics[topic]; ok {
		err, stop = broker.invokeLocked(handlers, src, msg, raw, err)
		if stop {
			return err
		}
	}
	// Continue to invoke all handlers.
	err, _ = broker.invokeLocked(broker.allTopicsHandlers, src, msg, raw, err)
	return err
}

func (broker *MessageBroker[S, R, T]) Subscribe(topic string, handler scheduling.MessageBrokerHandler[S, T, R]) {
	broker.topicMutex.Lock()
	defer broker.topicMutex.Unlock()

	broker.topics[topic] = append(broker.topics[topic], handler) // Should be nil-safe.
	if topic == MessageBrokerAllTopics {
		broker.allTopicsHandlers = broker.topics[topic]
	}
}

func (broker *MessageBroker[S, R, T]) Unsubscribe(topic string, handler scheduling.MessageBrokerHandler[S, T, R]) {
	broker.topicMutex.Lock()
	defer broker.topicMutex.Unlock()

	if handlers, ok := broker.topics[topic]; ok {
		if i := broker.findInHandlers(handler, handlers); i >= 0 {
			broker.topics[topic] = append(handlers[:i], handlers[i+1:]...)
			if topic == MessageBrokerAllTopics {
				broker.allTopicsHandlers = broker.topics[topic]
			}
		}
	}
}

func (broker *MessageBroker[S, R, T]) invokeLocked(handlers []scheduling.MessageBrokerHandler[S, T, R], src S, msg T, raw R, defaultErr error) (err error, stop bool) {
	// Later added handlers will be called first.
	err = defaultErr
	for i := len(handlers) - 1; i >= 0; i-- {
		err = handlers[i](src, msg, raw)
		if err, stop = broker.shouldStop(err); stop {
			return err, stop
		}
	}
	return
}

func (broker *MessageBroker[S, R, T]) findInHandlers(needle scheduling.MessageBrokerHandler[S, T, R], stack []scheduling.MessageBrokerHandler[S, T, R]) (pos int) {
	pos = -1
	n := reflect.ValueOf(needle).Pointer()
	for i, h := range stack {
		if reflect.ValueOf(h).Pointer() == n {
			return i
		}
	}
	return
}

func (broker *MessageBroker[S, R, T]) shouldStop(err error) (error, bool) {
	if errors.Is(err, types.ErrStopPropagation) {
		return nil, true
	} else if err != nil {
		return err, true
	}
	return nil, false
}
