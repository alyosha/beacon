package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestReceiveScheduled(t *testing.T) {
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

	psTest, err := setupTestPubsubEnv(ctx, conn)
	if err != nil {
		t.Fatalf("failed to setup pubsub env: %s", err)
	}

	connOpt := option.WithGRPCConn(conn)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer func() {
				cancel2()
				time.Sleep(1 * time.Second)
			}()

			var testEvt testEvent

			doneCh := make(chan struct{})

			cfg := testConfig
			cfg.Opts = []option.ClientOption{connOpt}
			cfg.PullInterval = 60 * time.Second
			cfg.Handlers = EventHandlers{
				dummyEventID: func(data []byte) (bool, error) {
					evt := testEvent{}
					if err := json.Unmarshal(data, &evt); err != nil {
						return true, err
					}

					if evt.ErrStr != "" {
						return true, errors.New(evt.ErrStr)
					}

					testEvt = evt

					doneCh <- struct{}{}

					return true, nil
				},
			}

			bcn, err := NewCloudPubsub(ctx2, cfg)
			if err != nil {
				t.Fatalf("unexpected err: %s", err)
			}

			errCh := bcn.Receive(ctx2)

			if err := publishTestEvt(ctx2, tc, psTest.topic); err != nil {
				t.Fatalf("failed to publish test event: %s", err)
			}

			select {
			case <-doneCh:
				if tc.wantTestEventVal != testEvt.Val {
					t.Fatalf("expected val: %s, got: %s", tc.wantTestEventVal, testEvt.Val)
				}
				return
			case err := <-errCh:
				if err.Error() != tc.wantErr.Error() {
					t.Fatalf("want: %v, got: %v", tc.wantErr, err)
				}
				return
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting on receive")
			}
			return
		})
	}
}
