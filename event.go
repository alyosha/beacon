package pharos

import (
	"encoding/json"
	"errors"
	"fmt"
)

type BeaconEvent struct {
	EventType    interface{}
	EventPayload string
}

type EventHandlers map[interface{}]func([]byte) (bool, error)

var ErrUnrecognizedEvent error = errors.New("pulled message of unrecognized event type")

func PrepareBeaconEvent(event, eventType interface{}) ([]byte, error) {
	payload, err := json.Marshal(&event)
	if err != nil {
		return nil, err
	}

	beaconEvt := BeaconEvent{
		EventType:    eventType,
		EventPayload: string(payload),
	}

	marshalledEvt, err := json.Marshal(beaconEvt)
	if err != nil {
		return nil, err
	}

	return marshalledEvt, nil
}

func process(evt BeaconEvent, handlerMap EventHandlers) (bool, error) {
	handler, ok := handlerMap[evt.EventType]
	if !ok {
		// TODO: make policy on unrecognized message acking configurable
		return false, fmt.Errorf("%w: %s", ErrUnrecognizedEvent, fmt.Sprintf("%v", evt.EventType))
	}

	return handler([]byte(evt.EventPayload))
}
