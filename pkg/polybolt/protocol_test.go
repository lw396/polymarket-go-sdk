package polybolt

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/gorilla/websocket"
)

func testEvent(channel Channel, seq uint64) Event {
	window := 0
	if channel == CryptoTWAP {
		window = 60
	}
	return Event{
		Version: 1, Channel: channel, Seq: seq, Timestamp: 1788973002000,
		Payload: Payload{
			PricePoint: PricePoint{
				Value: 64120.7, FullAccuracyValue: "64120.700000000000123456", Timestamp: 1788973002000,
			},
			Symbol: "btcusd", Source: "chainlink", WindowSeconds: window,
		},
	}
}

func TestValidateEvent(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Event)
	}{
		{name: "version", edit: func(e *Event) { e.Version = 2 }},
		{name: "sequence", edit: func(e *Event) { e.Seq = 0 }},
		{name: "time", edit: func(e *Event) { e.Timestamp = 0 }},
		{name: "wrong symbol", edit: func(e *Event) { e.Payload.Symbol = "ethusd" }},
		{name: "wrong channel", edit: func(e *Event) { e.Channel = "price.equity" }},
		{name: "wrong window", edit: func(e *Event) { e.Payload.WindowSeconds = 30 }},
		{name: "missing source", edit: func(e *Event) { e.Payload.Source = "" }},
		{name: "wrong TWAP source", edit: func(e *Event) { e.Payload.Source = "binance" }},
		{name: "missing exact price", edit: func(e *Event) { e.Payload.FullAccuracyValue = "" }},
		{name: "bad exact price", edit: func(e *Event) { e.Payload.FullAccuracyValue = "NaN" }},
		{name: "negative price", edit: func(e *Event) { e.Payload.FullAccuracyValue = "-1" }},
		{name: "missing point time", edit: func(e *Event) { e.Payload.Timestamp = 0 }},
		{name: "snapshot missing data", edit: func(e *Event) { e.Snapshot = true }},
		{name: "live has data", edit: func(e *Event) { e.Payload.Data = []PricePoint{} }},
		{name: "bad snapshot point", edit: func(e *Event) {
			e.Snapshot = true
			e.Payload.Data = []PricePoint{{FullAccuracyValue: "bad", Timestamp: 1}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{id: 1, subs: testConfig().Subscriptions, sequences: make(map[Channel]uint64)}
			e := testEvent(CryptoTWAP, 1)
			tt.edit(&e)
			if err := s.validateEvent(&e); !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected protocol error, got %v", err)
			}
		})
	}
}

func TestSequencesArePerChannelNotSymbol(t *testing.T) {
	subs := append(testConfig().Subscriptions, Subscription{Channel: CryptoPrices, Filter: Filter{Symbol: "ethusd"}})
	s := &session{id: 7, subs: subs, sequences: make(map[Channel]uint64)}
	events := []Event{
		testEvent(CryptoPrices, 1),
		testEvent(CryptoTWAP, 1),
		testEvent(CryptoPrices, 2),
		testEvent(CryptoPrices, 5),
	}
	events[2].Payload.Symbol = "ethusd"
	for i := range events {
		if err := s.validateEvent(&events[i]); err != nil {
			t.Fatal(err)
		}
		if events[i].ConnectionID != 7 {
			t.Fatal("missing connection ID")
		}
	}
	if events[3].Gap != 2 || events[2].Gap != 0 {
		t.Fatalf("incorrect gap: %d, %d", events[3].Gap, events[2].Gap)
	}
	if err := s.validateEvent(&events[3]); !errors.Is(err, ErrProtocol) {
		t.Fatal("duplicate sequence accepted")
	}
}

func TestEmptySnapshotAndExactPrice(t *testing.T) {
	s := &session{id: 1, subs: testConfig().Subscriptions, sequences: make(map[Channel]uint64)}
	e := testEvent(CryptoTWAP, 1)
	e.Snapshot = true
	e.Payload.Data = []PricePoint{}
	if err := s.validateEvent(&e); err != nil {
		t.Fatal(err)
	}
	p := testEvent(CryptoTWAP, 2).Payload.PricePoint
	got, err := p.Decimal()
	if err != nil || got.String() != "64120.700000000000123456" {
		t.Fatalf("decimal lost precision or was E18-scaled: %s, %v", got, err)
	}
	var decoded Event
	if err := json.Unmarshal([]byte(`{"v":1,"payload":{"full_accuracy_value":"10.001","timestamp":1}}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Payload.FullAccuracyValue != "10.001" {
		t.Fatal("embedded point not decoded")
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{err: ErrProtocol},
		{err: ErrSlowConsumer},
		{err: &RejectedError{Code: "auth_invalid"}},
		{err: &RejectedError{Code: "bad_filter"}},
		{err: &RejectedError{Code: "auth_unavailable"}, want: true},
		{err: &CloseError{Code: 4001}},
		{err: &CloseError{Code: 4008}},
		{err: &CloseError{Code: 1008}},
		{err: &CloseError{Code: 4003}, want: true},
		{err: &CloseError{Code: 4002}, want: true},
		{err: errors.New("transport"), want: true},
	}
	for _, tt := range tests {
		if got := retryable(tt.err); got != tt.want {
			t.Errorf("%v: got %t, want %t", tt.err, got, tt.want)
		}
	}
	err := safeSocketError(&websocket.CloseError{Code: 4001, Text: "test-secret"})
	if err.Error() != "polybolt: websocket closed (code=4001)" {
		t.Fatalf("unsafe close error: %v", err)
	}
}
