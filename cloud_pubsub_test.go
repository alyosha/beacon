package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dummyProjectID      = "my-fake-project"
	dummyTopicID        = "fake-topic"
	dummySubscriptionID = "fake-subscription"
	dummyPullInterval   = 120 * time.Second
	dummyEventID        = "dummy-event"
)

type psTestResources struct {
	client       *pubsub.Client
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
}

type testEvent struct {
	Val    string
	ErrStr string
}

type failEvent struct {
	Foo string
}

func (f *failEvent) Reset()         {}
func (f *failEvent) String() string { return "" }
func (f *failEvent) ProtoMessage()  {}

type testCase struct {
	description                  string
	publishEvent                 interface{}
	wantTestEventVal             string
	publishNonBeacon             bool
	publishProto                 bool
	publishUnrecognizedEventType bool
	nonBeaconEventType           string
	wantErr                      error
}

var testConfig = CloudPubsubConfig{
	ProjectID:      dummyProjectID,
	SubscriptionID: dummySubscriptionID,
	MaxMessages:    100,
}

var dummyEventHandlers = EventHandlers{
	dummyEventID: func(data []byte) (bool, error) {
		return true, nil
	},
}

var testCases = []testCase{
	{
		description: "[SUCCESS] event pulled and processed without error",
		publishEvent: testEvent{
			Val: "case 1",
		},
		wantTestEventVal: "case 1",
	},
	{
		description: "[SUCCESS] event pulled and processed, err returned from handler",
		publishEvent: testEvent{
			Val:    "case 2",
			ErrStr: "something happened",
		},
		wantErr: errors.New("something happened"),
	},
	{
		description: "[FAILURE]  err for non-BeaconEvent JSON event: missing event type",
		publishEvent: testEvent{
			Val: "case 3",
		},
		publishNonBeacon: true,
		wantErr:          ErrUnrecognizedEventType,
	},
	{
		description: "[FAILURE] unrecognized beacon event type",
		publishEvent: testEvent{
			Val: "case 4",
		},
		publishUnrecognizedEventType: true,
		wantErr:                      fmt.Errorf("%w: %s", ErrUnrecognizedEventType, "unknown"),
	},
	{
		description:  "[FAILURE] unmarshal err for non-BeaconEvent proto event",
		publishProto: true,
		wantErr:      fmt.Errorf("JSON unmarshal err: %w", errors.New("unexpected end of JSON input")),
	},
}

func TestNewCloudPubsubBeacon(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := pstest.NewServer()
	defer srv.Close()

	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("failed to connect to server: %s", err)
	}
	defer conn.Close()

	if _, err := setupTestPubsubEnv(ctx, conn); err != nil {
		t.Fatalf("failed to setup pubsub env: %s", err)
	}

	connOpt := option.WithGRPCConn(conn)

	testCases := []struct {
		description  string
		config       CloudPubsubConfig
		expectedType cloudPubsubBeaconType
		wantErr      error
	}{
		{
			description: "[FAILURE][SCHEDULED]: project ID not configured",
			config: CloudPubsubConfig{
				SubscriptionID: dummySubscriptionID,
				Handlers:       dummyEventHandlers,
				PullInterval:   dummyPullInterval,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			wantErr: errInvalidProjectID,
		},
		{
			description: "[FAILURE][SCHEDULED]: subscription ID not configured",
			config: CloudPubsubConfig{
				ProjectID:    dummyProjectID,
				Handlers:     dummyEventHandlers,
				PullInterval: dummyPullInterval,
				MaxMessages:  100,
				Opts:         []option.ClientOption{connOpt},
			},
			wantErr: errInvalidSubscriptionID,
		},
		{
			description: "[FAILURE][SCHEDULED]: event handlers not configured",
			config: CloudPubsubConfig{
				ProjectID:      dummyProjectID,
				SubscriptionID: dummySubscriptionID,
				PullInterval:   dummyPullInterval,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			wantErr: errInvalidEventHandlers,
		},
		{
			description: "[FAILURE][SCHEDULED]: max messages not configured",
			config: CloudPubsubConfig{
				ProjectID:      dummyProjectID,
				SubscriptionID: dummySubscriptionID,
				Handlers:       dummyEventHandlers,
				PullInterval:   dummyPullInterval,
				Opts:           []option.ClientOption{connOpt},
			},
			wantErr: errInvalidMaxMessages,
		},
		{
			description: "[SUCCESS][SCHEDULED] new beacon returned",
			config: CloudPubsubConfig{
				ProjectID:      dummyProjectID,
				SubscriptionID: dummySubscriptionID,
				Handlers:       dummyEventHandlers,
				PullInterval:   dummyPullInterval,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			expectedType: scheduledType,
		},
		{
			description: "[FAILURE][STREAM]: project ID not configured",
			config: CloudPubsubConfig{
				SubscriptionID: dummySubscriptionID,
				Handlers:       dummyEventHandlers,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			wantErr: errInvalidProjectID,
		},
		{
			description: "[FAILURE][STREAM]: subscription ID not configured",
			config: CloudPubsubConfig{
				ProjectID:   dummyProjectID,
				Handlers:    dummyEventHandlers,
				MaxMessages: 100,
				Opts:        []option.ClientOption{connOpt},
			},
			wantErr: errInvalidSubscriptionID,
		},
		{
			description: "[FAILURE][STREAM]: event handlers not configured",
			config: CloudPubsubConfig{
				ProjectID:      dummyProjectID,
				SubscriptionID: dummySubscriptionID,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			wantErr: errInvalidEventHandlers,
		},
		{
			description: "[SUCCESS][STREAM]: project ID not configured",
			config: CloudPubsubConfig{
				ProjectID:      dummyProjectID,
				SubscriptionID: dummySubscriptionID,
				Handlers:       dummyEventHandlers,
				MaxMessages:    100,
				Opts:           []option.ClientOption{connOpt},
			},
			expectedType: streamType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			beacon, err := NewCloudPubsub(ctx, tc.config)
			if err != nil {
				if tc.wantErr == nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if err.Error() != tc.wantErr.Error() {
					t.Fatalf("expected error value: %v, got: %v", tc.wantErr, err)
				}
				return
			}

			if beacon.beaconType != tc.expectedType {
				t.Fatalf(
					"expected beacon type: %s, got: %s",
					tc.expectedType,
					beacon.beaconType,
				)
			}

			if beacon.BeaconType() != string(tc.expectedType) {
				t.Fatal("beacon not properly confured to return type")
			}

			_, ok := beacon.handlerMap[dummyEventID]
			if !ok {
				t.Fatalf("expected handler method %s is not present in beacon", dummyEventID)
			}
		})
	}
}

func setupTestPubsubEnv(ctx context.Context, conn *grpc.ClientConn) (psTestResources, error) {
	testClient, err := pubsub.NewClient(ctx, dummyProjectID, option.WithGRPCConn(conn))
	if err != nil {
		return psTestResources{}, fmt.Errorf("failed to set up client: %s\n", err)
	}

	testTopic, err := testClient.CreateTopic(ctx, dummyTopicID)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok || grpcStatus.Code() != codes.AlreadyExists {
			if err != nil {
				return psTestResources{}, fmt.Errorf("failed to create topic\n")
			}
		}
	}

	subCfg := pubsub.SubscriptionConfig{Topic: testTopic}
	testSubscription, err := testClient.CreateSubscription(ctx, dummySubscriptionID, subCfg)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok || grpcStatus.Code() != codes.AlreadyExists {
			if err != nil {
				return psTestResources{}, fmt.Errorf("failed to create subscription\n")
			}
		}
	}

	go func() {
		defer testClient.Close()

		if testTopic != nil {
			defer testTopic.Delete(ctx)
		}

		if testSubscription != nil {
			defer testSubscription.Delete(ctx)
		}

		select {
		case <-ctx.Done():
		}
	}()

	return psTestResources{
		client:       testClient,
		topic:        testTopic,
		subscription: testSubscription,
	}, nil
}

func publishTestEvt(ctx context.Context, tc testCase, topic *pubsub.Topic) error {
	evtType := dummyEventID
	if tc.publishUnrecognizedEventType {
		evtType = "unknown"
	}

	beaconEvt, err := PrepareBeaconEvent(
		tc.publishEvent,
		evtType,
	)

	if tc.publishNonBeacon {
		beaconEvt, err = json.Marshal(&tc.publishEvent)
		if err != nil {
			return err
		}
	}

	if tc.publishProto {
		beaconEvt, err = proto.Marshal(&failEvent{
			Foo: "case 3",
		})
		if err != nil {
			return err
		}
	}

	rs := topic.Publish(ctx, &pubsub.Message{Data: beaconEvt})
	if _, err := rs.Get(ctx); err != nil {
		return err
	}

	return nil
}
