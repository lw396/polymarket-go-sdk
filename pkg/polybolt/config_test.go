package polybolt

import (
	"strings"
	"testing"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/auth"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Credentials = auth.APIKey{Key: "test-key", Secret: "test-secret", Passphrase: "test-passphrase"}
	cfg.Subscriptions = []Subscription{
		{Channel: CryptoPrices, Filter: Filter{Symbol: "btcusd"}},
		{Channel: CryptoTWAP, Filter: Filter{Symbol: "btcusd", WindowSeconds: 60}},
	}
	cfg.ReconnectDelay = time.Millisecond
	return cfg
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{name: "URL", edit: func(c *Config) { c.URL = "https://example.com" }},
		{name: "URL userinfo", edit: func(c *Config) { c.URL = "wss://user:secret@example.com/ws" }},
		{name: "credentials", edit: func(c *Config) { c.Credentials.Secret = "" }},
		{name: "empty subscriptions", edit: func(c *Config) { c.Subscriptions = nil }},
		{name: "too many", edit: func(c *Config) { c.Subscriptions = make([]Subscription, 65) }},
		{name: "duplicate", edit: func(c *Config) { c.Subscriptions[1] = c.Subscriptions[0] }},
		{name: "slash symbol", edit: func(c *Config) { c.Subscriptions[0].Filter.Symbol = "btc/usd" }},
		{name: "USDT symbol", edit: func(c *Config) { c.Subscriptions[0].Filter.Symbol = "btcusdt" }},
		{name: "uppercase", edit: func(c *Config) { c.Subscriptions[0].Filter.Symbol = "BTCUSD" }},
		{name: "empty base", edit: func(c *Config) { c.Subscriptions[0].Filter.Symbol = "usd" }},
		{name: "unsupported channel", edit: func(c *Config) { c.Subscriptions[0].Channel = "price.equity" }},
		{name: "spot window", edit: func(c *Config) { c.Subscriptions[0].Filter.WindowSeconds = 60 }},
		{name: "30 second TWAP", edit: func(c *Config) { c.Subscriptions[1].Filter.WindowSeconds = 30 }},
		{name: "missing TWAP window", edit: func(c *Config) { c.Subscriptions[1].Filter.WindowSeconds = 0 }},
		{name: "provider", edit: func(c *Config) { c.Subscriptions[0].Filter.Provider = "unknown" }},
		{name: "negative timeout", edit: func(c *Config) { c.HandshakeTimeout = -1 }},
		{name: "negative read timeout", edit: func(c *Config) { c.ReadTimeout = -1 }},
		{name: "negative delay", edit: func(c *Config) { c.ReconnectDelay = -1 }},
		{name: "negative buffer", edit: func(c *Config) { c.BufferSize = -1 }},
		{name: "invalid attempts", edit: func(c *Config) { c.MaxReconnectAttempts = -2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.edit(&cfg)
			if _, err := cfg.normalize(); err == nil {
				t.Fatal("expected validation error")
			} else if strings.Contains(err.Error(), cfg.Credentials.Secret) && cfg.Credentials.Secret != "" {
				t.Fatal("error exposed credentials")
			}
		})
	}
}

func TestConfigDefaultsAndCopies(t *testing.T) {
	cfg := testConfig()
	cfg.URL = ""
	cfg.HandshakeTimeout = 0
	cfg.ReadTimeout = 0
	cfg.BufferSize = 0
	cfg.MaxReconnectAttempts = 0
	cfg.ReconnectDelay = 0
	normalized, err := cfg.normalize()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.URL != ProdURL || normalized.BufferSize != 256 || normalized.MaxReconnectAttempts != 5 {
		t.Fatalf("unexpected defaults: URL=%s buffer=%d attempts=%d",
			normalized.URL, normalized.BufferSize, normalized.MaxReconnectAttempts)
	}
	cfg.Subscriptions[0].Filter.Symbol = "ethusd"
	if normalized.Subscriptions[0].Filter.Symbol != "btcusd" {
		t.Fatal("subscriptions alias caller slice")
	}
}
