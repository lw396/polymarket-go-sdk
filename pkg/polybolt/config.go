package polybolt

import (
	"errors"
	"net/url"
	"slices"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/auth"
	"github.com/gorilla/websocket"
)

const (
	// ProdURL is the production PolyBolt endpoint.
	ProdURL = "wss://ws-live-v2.polymarket.com/ws"
	// MaxSubscriptions is the per-connection subscription limit.
	MaxSubscriptions = 64
)

// Config describes one authenticated connection with a fixed set of subscriptions.
// The zero timeouts and buffer size select defaults. A nil Dialer uses a copy of
// websocket.DefaultDialer. Credentials and subscriptions are copied by Dial.
type Config struct {
	URL              string
	Credentials      auth.APIKey
	Subscriptions    []Subscription
	Dialer           *websocket.Dialer
	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	BufferSize       int
	// Reconnect disables automatic reconnect when false.
	Reconnect bool
	// ReconnectDelay defaults to two seconds.
	ReconnectDelay time.Duration
	// MaxReconnectAttempts defaults to five; -1 means unlimited.
	MaxReconnectAttempts int
}

// DefaultConfig returns settings without credentials or subscriptions.
func DefaultConfig() Config {
	return Config{
		URL: ProdURL, HandshakeTimeout: 30 * time.Second,
		ReadTimeout: 75 * time.Second, BufferSize: 256,
		Reconnect: true, ReconnectDelay: 2 * time.Second, MaxReconnectAttempts: 5,
	}
}

func (cfg Config) normalize() (Config, error) {
	defaults := DefaultConfig()
	if cfg.URL == "" {
		cfg.URL = defaults.URL
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil {
		return cfg, errors.New("polybolt: URL must be a ws:// or wss:// endpoint without user info")
	}
	key := cfg.Credentials
	if key.Key == "" || key.Secret == "" || key.Passphrase == "" {
		return cfg, errors.New("polybolt: complete CLOB API credentials are required")
	}
	if len(cfg.Subscriptions) == 0 || len(cfg.Subscriptions) > MaxSubscriptions {
		return cfg, errors.New("polybolt: expected between 1 and 64 subscriptions")
	}
	cfg.Subscriptions = slices.Clone(cfg.Subscriptions)
	seen := make(map[Subscription]bool, len(cfg.Subscriptions))
	for _, sub := range cfg.Subscriptions {
		if err := sub.validate(); err != nil {
			return cfg, err
		}
		if seen[sub] {
			return cfg, errors.New("polybolt: duplicate subscription")
		}
		seen[sub] = true
	}
	if cfg.HandshakeTimeout < 0 || cfg.ReadTimeout < 0 || cfg.ReconnectDelay < 0 || cfg.BufferSize < 0 {
		return cfg, errors.New("polybolt: timeouts, reconnect delay and buffer size cannot be negative")
	}
	if cfg.MaxReconnectAttempts < -1 {
		return cfg, errors.New("polybolt: MaxReconnectAttempts must be -1 or non-negative")
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = defaults.HandshakeTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaults.ReadTimeout
	}
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = defaults.ReconnectDelay
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaults.BufferSize
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = defaults.MaxReconnectAttempts
	}
	dialer := websocket.DefaultDialer
	if cfg.Dialer != nil {
		dialer = cfg.Dialer
	}
	copyDialer := *dialer
	cfg.Dialer = &copyDialer
	return cfg, nil
}
