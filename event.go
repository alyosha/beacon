package beacon

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

var ErrUnrecognizedEventType error = errors.New("pulled message of unrecognized event type")

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

func process(evt BeaconEvent, handlerMap EventHandlers, ackUnrecognized bool) (bool, error) {
	handler, ok := handlerMap[evt.EventType]
	if !ok {
		if evt.EventType == nil {
			return ackUnrecognized, ErrUnrecognizedEventType
		}
		return ackUnrecognized, fmt.Errorf("%w: %s", ErrUnrecognizedEventType, fmt.Sprintf("%v", evt.EventType))
	}

	return handler([]byte(evt.EventPayload))
}
