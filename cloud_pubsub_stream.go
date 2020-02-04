package beacon

import (
	"context"
	"io"

	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func (b *CloudPubsubBeacon) runStream(ctx context.Context, errCh chan error) {
	reqCh := make(chan *pubsubpb.StreamingPullRequest)
	ackIDCh := make(chan string)
	doneCh := make(chan struct{})

	go func() {
		b.streamSend(reqCh, errCh)
		doneCh <- struct{}{}
	}()

	go handleAcks(ackIDCh, reqCh)

	go func() {
		for {
			if b.contextCancelled {
				break
			}
			b.streamReceive(ackIDCh, errCh)
		}
		close(ackIDCh)
	}()

	reqCh <- b.pubsub.openStreamReq

	_ = <-ctx.Done()
	b.contextCancelled = true

	_ = <-doneCh
	close(errCh)

	return
}

func (b *CloudPubsubBeacon) streamSend(reqCh chan *pubsubpb.StreamingPullRequest, errCh chan error) {
	for req := range reqCh {
		if err := b.pubsub.stream.Send(req); err != nil {
			errCh <- err
		}
	}
	b.pubsub.stream.CloseSend()
}

func (b *CloudPubsubBeacon) streamReceive(ackIDCh chan string, errCh chan error) {
	resp, err := b.pubsub.stream.Recv()
	if err == io.EOF {
		errCh <- err
		return
	}

	if err != nil {
		errCh <- err
		return
	}

	if resp == nil {
		return
	}

	ackIDs, err := b.processMessages(resp.ReceivedMessages)
	if err != nil {
		errCh <- err
		return
	}

	for _, ackID := range ackIDs {
		ackIDCh <- ackID
	}
}

func handleAcks(ackIDCh chan string, reqCh chan *pubsubpb.StreamingPullRequest) {
	for ackID := range ackIDCh {
		reqCh <- &pubsubpb.StreamingPullRequest{AckIds: []string{ackID}}
	}
	close(reqCh)
}
