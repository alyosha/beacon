package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

const subPattern = "projects/%s/subscriptions/%s"

type cloudPubsubBeaconType string

const (
	scheduledType cloudPubsubBeaconType = "scheduled"
	streamType    cloudPubsubBeaconType = "stream"
)

const defaultAckDeadline int32 = 10

type CloudPubsubBeacon struct {
	beaconType       cloudPubsubBeaconType
	pubsub           pubsubHandler
	handlerMap       EventHandlers
	contextCancelled bool
}

type CloudPubsubConfig struct {
	ProjectID                string
	SubscriptionID           string
	Handlers                 EventHandlers
	AckUnrecognized          bool
	StreamAckDeadlineSeconds time.Duration
	PullInterval             time.Duration
	MaxMessages              int32
	Opts                     []option.ClientOption
}

type pubsubHandler struct {
	subPath         string
	subscriber      *pubsubv1.SubscriberClient
	stream          pubsubpb.Subscriber_StreamingPullClient
	openStreamReq   *pubsubpb.StreamingPullRequest
	ackUnrecognized bool
	pullInterval    time.Duration
	maxMessages     int32
}

var (
	errInvalidProjectID      = errors.New("project ID cannot be blank")
	errInvalidSubscriptionID = errors.New("subscription ID cannot be blank")
	errInvalidEventHandlers  = errors.New("must provide at least one event handler method")
	errInvalidMaxMessages    = errors.New("max messages must be set to 1 or higher")
)

func NewCloudPubsub(ctx context.Context, cfg CloudPubsubConfig) (*CloudPubsubBeacon, error) {
	bcn, err := newCloudPubsubBeacon(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return bcn, nil
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

func (b *CloudPubsubBeacon) Close() {
	b.pubsub.subscriber.Close()
}

func newCloudPubsubBeacon(ctx context.Context, cfg CloudPubsubConfig) (*CloudPubsubBeacon, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}

	subClient, err := pubsubv1.NewSubscriberClient(ctx, cfg.Opts...)
	if err != nil {
		return nil, err
	}

	subscriptionPath := fmt.Sprintf(subPattern, cfg.ProjectID, cfg.SubscriptionID)

	bcn := &CloudPubsubBeacon{
		pubsub: pubsubHandler{
			subPath:         subscriptionPath,
			subscriber:      subClient,
			ackUnrecognized: cfg.AckUnrecognized,
			pullInterval:    cfg.PullInterval,
			maxMessages:     cfg.MaxMessages,
		},
		handlerMap: cfg.Handlers,
	}

	if cfg.PullInterval > 0 {
		if cfg.MaxMessages < 1 {
			return nil, errInvalidMaxMessages
		}
		bcn.beaconType = scheduledType
	} else {
		bcn.beaconType = streamType
		stream, err := subClient.StreamingPull(ctx)
		if err != nil {
			return nil, err
		}
		bcn.pubsub.stream = stream
		ackDeadline := defaultAckDeadline
		if cfg.StreamAckDeadlineSeconds != 0 {
			ackDeadline = int32(cfg.StreamAckDeadlineSeconds)
		}
		bcn.pubsub.openStreamReq = &pubsubpb.StreamingPullRequest{
			Subscription:             subscriptionPath,
			StreamAckDeadlineSeconds: ackDeadline,
		}
	}

	return bcn, nil
}

func validate(cfg CloudPubsubConfig) error {
	if cfg.ProjectID == "" {
		return errInvalidProjectID
	}

	if cfg.SubscriptionID == "" {
		return errInvalidSubscriptionID
	}

	if len(cfg.Handlers) < 1 {
		return errInvalidEventHandlers
	}

	return nil
}

func (b *CloudPubsubBeacon) processMessages(messages []*pubsubpb.ReceivedMessage) ([]string, error) {
	var eg errgroup.Group

	ackIDCh := make(chan string)
	doneCh := make(chan struct{})

	var ackIDs []string
	go func() {
		for ackID := range ackIDCh {
			ackIDs = append(ackIDs, ackID)
		}
		doneCh <- struct{}{}
	}()

	for _, msg := range messages {
		msg := msg
		eg.Go(func() error {
			evt := BeaconEvent{}
			if err := json.Unmarshal([]byte(msg.Message.Data), &evt); err != nil {
				ackIDCh <- msg.AckId
				return fmt.Errorf("JSON unmarshal err: %w", err)
			}
			shouldAck, err := process(evt, b.handlerMap, b.pubsub.ackUnrecognized)
			if shouldAck {
				ackIDCh <- msg.AckId
			}
			return err
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	close(ackIDCh)

	_ = <-doneCh

	return ackIDs, nil
}
