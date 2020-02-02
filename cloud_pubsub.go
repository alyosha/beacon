package pharos

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
)

const subPattern = "projects/%s/subscriptions/%s"

type cloudPubsubBeaconType string

const (
	scheduledType cloudPubsubBeaconType = "scheduled"
	streamType    cloudPubsubBeaconType = "stream"
)

type CloudPubsubBeacon struct {
	beaconType   cloudPubsubBeaconType
	scheduledSub scheduledSubscriber
	streamSub    *pubsub.Subscription
	handlerMap   EventHandlers
}

type CloudPubsubBeaconConfig struct {
	ProjectID       string
	SubscriptionID  string
	Handlers        EventHandlers
	PullInterval    time.Duration
	MaxMessages     int32
	ReceiveSettings pubsub.ReceiveSettings
}

func NewCloudPubsubBeacon(ctx context.Context, cfg CloudPubsubBeaconConfig) (*CloudPubsubBeacon, error) {
	if cfg.PullInterval > 0 {
		b, err := newScheduledBeacon(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return b, nil
	}

	return newStreamBeacon(ctx, cfg)
}

func (b *CloudPubsubBeacon) Receive(ctx context.Context) chan error {
	errCh := make(chan error)

	go func() {
		switch b.beaconType {
		case scheduledType:
			b.runScheduled(ctx, errCh)
		case streamType:
			b.runStream(ctx, errCh)
		}
	}()

	return errCh
}

func (b *CloudPubsubBeacon) BeaconType() string {
	return string(b.beaconType)
}

func validateCommonConfig(cfg CloudPubsubBeaconConfig) error {
	if cfg.ProjectID == "" {
		return errors.New("project ID cannot be blank")
	}

	if cfg.SubscriptionID == "" {
		return errors.New("subscription ID cannot be blank")
	}

	if len(cfg.Handlers) < 1 {
		return errors.New("must provide at least one event handler method")
	}

	return nil
}
