package forwarder

import (
	"encoding/json"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

// DecodedEventConsumer defines the interface of an entity that consumes decoded Docker events from an EventDecoder.
type DecodedEventConsumer interface {
	// ConsumeDecodedDockerEvent consumes an event from an EventDecoder.
	ConsumeDecodedDockerEvent(evt map[string]interface{})
}

// EventDecoder decodes Docker events before passing them along to a DecodedEventConsumer.
type EventDecoder struct {
	// events are the events observed/collected by an ContainerCreatedEventCollector and delivered to us
	// via our ConsumeDockerEvent method.
	events chan []byte

	// log is a simple logger.
	log logger.Logger

	consumer DecodedEventConsumer
}

func NewEventDecoder(consumer DecodedEventConsumer) *EventDecoder {
	decoder := &EventDecoder{
		events:   make(chan []byte, 5),
		consumer: consumer,
	}

	config.InitLogger(&decoder.log, decoder)

	return decoder
}

func (d *EventDecoder) ConsumeEncodedDockerEvent(evt []byte) {
	d.events <- evt
}

func (d *EventDecoder) ConsumeDecodedDockerEvent(_ map[string]interface{}) {
	panic("Not implemented")
}

func (d *EventDecoder) DecodeEvents() {
	for {
		encodedEvent := <-d.events

		var decodedEvent map[string]interface{}
		err := json.Unmarshal(encodedEvent, &decodedEvent)
		if err != nil {
			d.log.Error("Failed to decode Docker event: %v", err)
			continue
		}

		d.consumer.ConsumeDecodedDockerEvent(decodedEvent)
	}
}
