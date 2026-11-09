// Package polybolt provides authenticated crypto reference-price and 60-second
// TWAP streams. It is opt-in and does not replace the SDK's legacy RTDS client.
package polybolt

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/shopspring/decimal"
)

// Channel identifies a supported price stream.
type Channel string

const (
	CryptoPrices Channel = "price.crypto"
	CryptoTWAP   Channel = "price.crypto.twap"
)

var cryptoSymbol = regexp.MustCompile(`^[a-z0-9]+usd$`)

// Filter selects a canonical USD symbol, for example btcusd.
// Provider is a request, not a guarantee; always inspect Payload.Source.
type Filter struct {
	Symbol        string `json:"symbol"`
	WindowSeconds int    `json:"window_seconds,omitzero"`
	Provider      string `json:"provider,omitempty"`
}

// Subscription is one channel/filter pair. TWAP requires WindowSeconds=60.
type Subscription struct {
	Channel Channel `json:"channel"`
	Filter  Filter  `json:"filter"`
}

func (s Subscription) validate() error {
	if !cryptoSymbol.MatchString(s.Filter.Symbol) || strings.HasSuffix(s.Filter.Symbol, "usdt") {
		return errors.New("polybolt: crypto symbol must be a lowercase USD pair such as btcusd")
	}
	switch s.Channel {
	case CryptoPrices:
		if s.Filter.WindowSeconds != 0 {
			return errors.New("polybolt: spot subscription cannot specify window_seconds")
		}
	case CryptoTWAP:
		if s.Filter.WindowSeconds != 60 {
			return errors.New("polybolt: TWAP subscription requires window_seconds=60")
		}
	default:
		return errors.New("polybolt: unsupported channel")
	}
	switch s.Filter.Provider {
	case "", "chainlink", "pyth", "binance":
		return nil
	default:
		return errors.New("polybolt: unsupported provider")
	}
}

// PricePoint preserves the source's exact decimal price. FullAccuracyValue is
// already in price units: do NOT divide it by 1e18 as with legacy RTDS TWAP.
type PricePoint struct {
	Value             float64 `json:"value"`
	FullAccuracyValue string  `json:"full_accuracy_value"`
	Timestamp         int64   `json:"timestamp"` // Unix milliseconds
}

// Decimal parses the exact price without a float64 round-trip.
func (p PricePoint) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromString(p.FullAccuracyValue)
}

// Payload contains either a live PricePoint or snapshot Data (possibly empty).
// Source and WindowSeconds apply to every point in a snapshot.
type Payload struct {
	PricePoint
	Symbol        string `json:"symbol"`
	Source        string `json:"source"`
	WindowSeconds int    `json:"window_seconds,omitzero"`
	// Omit nil on live updates, but preserve [] on empty snapshots.
	Data []PricePoint `json:"data,omitzero"`
}

// Event is one server envelope, including snapshots and loss indicators.
type Event struct {
	Version   int     `json:"v"`
	Channel   Channel `json:"channel"`
	Seq       uint64  `json:"seq"`
	Timestamp int64   `json:"ts"` // Unix milliseconds
	Snapshot  bool    `json:"snapshot,omitzero"`
	Dropped   uint64  `json:"dropped,omitzero"`
	Payload   Payload `json:"payload"`
	// ConnectionID increases after reconnect. Compare Seq only within the same
	// ConnectionID and Channel, not per symbol. Snapshots also consume Seq.
	ConnectionID uint64 `json:"-"`
	// Gap is the number of skipped sequence numbers before this envelope.
	// It and Dropped may describe the same loss; do not add them together.
	Gap uint64 `json:"-"`
}

var (
	// ErrSlowConsumer terminates a stream instead of silently dropping events.
	ErrSlowConsumer = errors.New("polybolt: local event buffer full")
	// ErrProtocol indicates a malformed or unexpected server message.
	ErrProtocol = errors.New("polybolt: invalid server message")
)

// RejectedError is a machine-readable server rejection. Raw frames and server
// free-text messages are deliberately excluded to avoid exposing credentials.
type RejectedError struct {
	Code    string
	Channel Channel
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("polybolt: request rejected (code=%s channel=%s)", e.Code, e.Channel)
}

// CloseError preserves the server close code without echoing its free-text reason.
type CloseError struct {
	Code int
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("polybolt: websocket closed (code=%d)", e.Code)
}

// ConnectionError reports an interrupted connection. Reconnecting means the SDK
// will attempt recovery; the next connection supplies new snapshots.
type ConnectionError struct {
	ConnectionID uint64
	Reconnecting bool
	Err          error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("polybolt: connection %d interrupted (reconnecting=%t): %v",
		e.ConnectionID, e.Reconnecting, e.Err)
}

func (e *ConnectionError) Unwrap() error { return e.Err }
