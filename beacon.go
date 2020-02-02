package beacon

import "context"

type Beacon interface {
	Receive(context.Context) error
	BeaconType() string
}
