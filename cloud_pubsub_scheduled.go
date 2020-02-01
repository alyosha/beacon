package pharos

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

type scheduledSubscriber struct {
	subscriptionPath string
	pubsubClient     *pubsubv1.SubscriberClient
	pullInterval     time.Duration
	maxMessages      int32
}

func newScheduledBeacon(ctx context.Context, cfg CloudPubsubBeaconConfig) (*CloudPubsubBeacon, error) {
	subClient, err := pubsubv1.NewSubscriberClient(ctx)
	if err != nil {
		return nil, err
	}

	subscriptionPath := fmt.Sprintf(subPattern, cfg.ProjectID, cfg.SubscriptionID)

	return &CloudPubsubBeacon{
		beaconType: scheduledType,
		scheduledSub: scheduledSubscriber{
			subscriptionPath: subscriptionPath,
			pubsubClient:     subClient,
			pullInterval:     cfg.PullInterval,
			maxMessages:      cfg.MaxMessages,
		},
		handlerMap: cfg.Handlers,
	}, nil
}

func (b *CloudPubsubBeacon) runScheduled(ctx context.Context, errCh chan error) error {
	for {
		go b.receive(ctx, errCh)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(b.scheduledSub.pullInterval):
			continue
		}
	}
}

func (b *CloudPubsubBeacon) receive(ctx context.Context, errCh chan error) {
	pullResp, err := b.pullMessageChunks(ctx)
	if err != nil {
		errCh <- err
		return
	}

	ackIDCh := make(chan string)
	var ackIDs []string
	go func() {
		for ackID := range ackIDCh {
			ackIDs = append(ackIDs, ackID)
		}
	}()

	var wg sync.WaitGroup
	for i, msg := range pullResp.ReceivedMessages {
		wg.Add(1)
		i, msg := i, msg
		go func(i int, msg *pubsubpb.ReceivedMessage) {
			defer wg.Done()
			evt := BeaconEvent{}
			if err := json.Unmarshal([]byte(msg.Message.Data), &evt); err != nil {
				errCh <- fmt.Errorf("JSON unmarshal err: %w", err)
				return
			}
			shouldAck, err := process(evt, b.handlerMap)
			if err != nil {
				errCh <- err
			}
			if shouldAck {
				ackIDCh <- msg.AckId
			}
		}(i, msg)
	}

	wg.Wait()

	close(ackIDCh)

	if len(ackIDs) > 0 {
		if err := b.ack(ctx, ackIDs); err != nil {
			errCh <- err
		}
	}
}

func (b *CloudPubsubBeacon) pullMessageChunks(ctx context.Context) (*pubsubpb.PullResponse, error) {
	req := pubsubpb.PullRequest{
		Subscription: b.scheduledSub.subscriptionPath,
		MaxMessages:  b.scheduledSub.maxMessages,
	}

	resp, err := b.scheduledSub.pubsubClient.Pull(ctx, &req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (b *CloudPubsubBeacon) ack(ctx context.Context, ackIDs []string) error {
	req := pubsubpb.AcknowledgeRequest{
		AckIds:       ackIDs,
		Subscription: b.scheduledSub.subscriptionPath,
	}

	return b.scheduledSub.pubsubClient.Acknowledge(ctx, &req)
}
