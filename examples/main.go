package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/alyosha/beacon"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/patrickmn/go-cache"
)

const (
	exitOK = iota
	exitError
)

const (
	maxMessages  = 100
	pullInterval = 30 * time.Second
)

type config struct {
	ProjectID      string `envconfig:"PROJECT_ID" required:"true"`
	TopicID        string `envconfig:"TOPIC_ID" required:"true"`
	SubscriptionID string `envconfig:"SUBSCRIPTION_ID" required:"true"`
	ScheduledType  bool   `envconfig:"SCHEDULED_TYPE" default:"true"`
}

var fatalErr = errors.New("urecoverable, shut down beacon")

type paymentEvent struct {
	ID          string
	Amount      float64
	Description string
	User        string
}

type locationEvent struct {
	ID     string
	Coords latLng
	User   string
}

type latLng struct {
	Lat float64
	Lng float64
}

var sampleUsers = []string{"steve", "michael", "john"}

var locations = map[string]latLng{
	"steve": latLng{
		Lat: float64(35.3952459),
		Lng: float64(138.7216633),
	},
	"michael": latLng{
		Lat: float64(30.3481033),
		Lng: float64(130.4538544),
	},
	"john": latLng{
		Lat: float64(42.9855478),
		Lng: float64(141.1075523),
	},
}

func main() {
	os.Exit(_main())
}

func _main() int {
	ctx, cancel := context.WithCancel(context.Background())

	var env config
	if err := envconfig.Process("", &env); err != nil {
		fmt.Printf("[ERR]: %s\n", err)
		return exitError
	}

	c := cache.New(5*time.Minute, 10*time.Minute)

	paymentEvtHandler := func(data []byte) (bool, error) {
		fmt.Printf("[INFO]: new payment message\n")
		evt := paymentEvent{}
		if err := json.Unmarshal(data, &evt); err != nil {
			return true, err
		}
		if evt.Amount > float64(400) {
			return true, fatalErr
		}
		c.Set(evt.ID, evt, cache.DefaultExpiration)

		fmt.Println(len(c.Items()))
		return true, nil
	}

	locationEvtHandler := func(data []byte) (bool, error) {
		fmt.Printf("[INFO]: new loc message\n")
		evt := locationEvent{}
		if err := json.Unmarshal(data, &evt); err != nil {
			return true, err
		}
		if evt.User == "james" {
			return true, errors.New("we don't like james")
		}
		c.Set(evt.ID, evt, cache.DefaultExpiration)
		fmt.Println(len(c.Items()))
		return true, nil
	}

	handlers := beacon.EventHandlers{
		"payment-event":  paymentEvtHandler,
		"location-event": locationEvtHandler,
	}

	cfg := beacon.CloudPubsubConfig{
		ProjectID:      env.ProjectID,
		SubscriptionID: env.SubscriptionID,
		Handlers:       handlers,
	}

	if env.ScheduledType {
		cfg.MaxMessages = maxMessages
		cfg.PullInterval = pullInterval
	}

	bcn, err := beacon.NewCloudPubsub(ctx, cfg)
	if err != nil {
		fmt.Printf("[ERR]: %s\n", err)
		return exitError
	}

	go publishEvents(ctx, env.ProjectID, env.TopicID)

	errCh := bcn.Receive(ctx)

	doneCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)

	go func() {
		for err := range errCh {
			fmt.Printf("[ERR]: %s\n", err)
			if isFatal(err) {
				doneCh <- struct{}{}
			}
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Println("context cancelled")
	case <-sigCh:
		fmt.Printf("\nexiting\n")
		cancel()
	case <-doneCh:
		fmt.Println("finished with fatal error")
		return exitError
	}

	fmt.Printf("[INFO] final cache size: %d entries", len(c.Items()))

	return exitOK
}

func isFatal(err error) bool {
	if err == fatalErr {
		return true
	}
	return false
}

func publishEvents(ctx context.Context, projectID, topicID string) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	topic := client.Topic(topicID)

	var beaconEvents [][]byte
	for i, user := range sampleUsers {
		paymentBeaconEvt, err := beacon.PrepareBeaconEvent(
			paymentEvent{
				ID:     uuid.New().String(),
				Amount: float64(float64(i+1) * 100.5),
				User:   user,
			},
			"payment-event",
		)

		locBeaconEvt, err := beacon.PrepareBeaconEvent(
			locationEvent{
				ID:     uuid.New().String(),
				User:   user,
				Coords: locations[user],
			},
			"location-event",
		)

		if err != nil {
			return err
		}

		beaconEvents = append(beaconEvents, paymentBeaconEvt, locBeaconEvt)
	}

	var wg sync.WaitGroup
	for _, evt := range beaconEvents {
		wg.Add(1)
		go func(evt []byte) {
			defer wg.Done()
			rs := topic.Publish(ctx, &pubsub.Message{Data: evt})
			if _, err := rs.Get(ctx); err != nil {
				fmt.Println(err)
			}
		}(evt)
	}

	wg.Wait()

	return nil
}
