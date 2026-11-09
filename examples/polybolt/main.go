// This read-only example requires existing CLOB API credentials. It never places orders.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/auth"
	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/polybolt"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg := polybolt.DefaultConfig()
	cfg.Credentials = auth.APIKey{
		Key: os.Getenv("POLY_API_KEY"), Secret: os.Getenv("POLY_API_SECRET"),
		Passphrase: os.Getenv("POLY_API_PASSPHRASE"),
	}
	cfg.Subscriptions = []polybolt.Subscription{
		{Channel: polybolt.CryptoPrices, Filter: polybolt.Filter{Symbol: "btcusd", Provider: "chainlink"}},
		{Channel: polybolt.CryptoTWAP, Filter: polybolt.Filter{Symbol: "btcusd", WindowSeconds: 60}},
	}
	client, err := polybolt.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	for event := range client.Events() {
		if event.Gap != 0 || event.Dropped != 0 {
			log.Printf("data loss: connection=%d channel=%s gap=%d dropped=%d",
				event.ConnectionID, event.Channel, event.Gap, event.Dropped)
		}
		if event.Snapshot {
			fmt.Printf("%s %s source=%s snapshot points=%d\n",
				event.Channel, event.Payload.Symbol, event.Payload.Source, len(event.Payload.Data))
			continue
		}
		fmt.Printf("%s %s source=%s time=%d price=%s\n",
			event.Channel, event.Payload.Symbol, event.Payload.Source,
			event.Payload.Timestamp, event.Payload.FullAccuracyValue)
	}
	return client.Err()
}
