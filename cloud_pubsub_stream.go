package pharos

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
)

func newStreamBeacon(ctx context.Context, cfg CloudPubsubBeaconConfig) (*CloudPubsubBeacon, error) {
	pubsubClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, err
	}

	subscription := pubsubClient.Subscription(cfg.SubscriptionID)
	ok, err := subscription.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, fmt.Errorf("subscription with ID %s does not exist", cfg.SubscriptionID)
	}

	subscription.ReceiveSettings = cfg.ReceiveSettings

	return &CloudPubsubBeacon{
		beaconType: streamType,
		streamSub:  subscription,
		handlerMap: cfg.Handlers,
	}, nil
}

func (b *CloudPubsubBeacon) runStream(ctx context.Context, errCh chan error) error {
	err := b.streamSub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		evt := BeaconEvent{}
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			errCh <- fmt.Errorf("JSON unmarshal err: %w", err)
			return
		}

		shouldAck, err := process(evt, b.handlerMap)
		if err != nil {
			errCh <- err
		}

		if shouldAck {
			msg.Ack()
		}

		return
	})

	return err
}
