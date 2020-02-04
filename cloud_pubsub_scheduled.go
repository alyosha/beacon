package beacon

import (
	"context"
	"time"

	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func (b *CloudPubsubBeacon) runScheduled(ctx context.Context, errCh chan error) {
	for {
		doneCh := make(chan struct{})
		go func() {
			b.receive(ctx, errCh)
			doneCh <- struct{}{}
		}()
		select {
		case <-ctx.Done():
			_ = <-doneCh
			close(errCh)
			return
		case <-time.After(b.pubsub.pullInterval):
		}
	}
}

func (b *CloudPubsubBeacon) receive(ctx context.Context, errCh chan error) {
	pullResp, err := b.pullMessageChunks(ctx)
	if err != nil {
		errCh <- err
		return
	}

	ackIDs, err := b.processMessages(pullResp.ReceivedMessages)
	if err != nil {
		errCh <- err
		return
	}

	if len(ackIDs) > 0 {
		if err := b.ack(ctx, ackIDs); err != nil {
			errCh <- err
		}
	}
}

func (b *CloudPubsubBeacon) pullMessageChunks(ctx context.Context) (*pubsubpb.PullResponse, error) {
	req := pubsubpb.PullRequest{
		Subscription: b.pubsub.subPath,
		MaxMessages:  b.pubsub.maxMessages,
	}

	resp, err := b.pubsub.subscriber.Pull(ctx, &req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (b *CloudPubsubBeacon) ack(ctx context.Context, ackIDs []string) error {
	req := pubsubpb.AcknowledgeRequest{
		AckIds:       ackIDs,
		Subscription: b.pubsub.subPath,
	}

	return b.pubsub.subscriber.Acknowledge(ctx, &req)
}
