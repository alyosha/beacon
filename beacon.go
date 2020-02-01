package pharos

import "context"

type Beacon interface {
	Receive(context.Context) error
	BeaconType() string
}
