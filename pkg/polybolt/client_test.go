package polybolt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func socketServer(t *testing.T, handler func(*websocket.Conn)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		stop := context.AfterFunc(t.Context(), func() { _ = conn.Close() })
		defer stop()
		defer func() { _ = conn.Close() }()
		handler(conn)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func acceptSubscriptions(t *testing.T, conn *websocket.Conn) []Subscription {
	t.Helper()
	var authFrame struct {
		Op   string            `json:"op"`
		RID  string            `json:"rid"`
		Auth map[string]string `json:"auth"`
	}
	if err := conn.ReadJSON(&authFrame); err != nil {
		t.Errorf("read authentication: %v", err)
		return nil
	}
	expected := testConfig().Credentials
	valid := authFrame.Op == "auth" && authFrame.Auth["apiKey"] == expected.Key &&
		authFrame.Auth["secret"] == expected.Secret && authFrame.Auth["passphrase"] == expected.Passphrase
	if !valid {
		t.Error("incorrect authentication frame")
		return nil
	}
	if err := conn.WriteJSON(acknowledgement{Op: "authed", RID: authFrame.RID}); err != nil {
		return nil
	}
	var request struct {
		Op            string         `json:"op"`
		RID           string         `json:"rid"`
		Subscriptions []Subscription `json:"subscriptions"`
	}
	if err := conn.ReadJSON(&request); err != nil {
		t.Errorf("read subscriptions: %v", err)
		return nil
	}
	if request.Op != "subscribe" || request.RID != "subscribe" || len(request.Subscriptions) == 0 {
		t.Error("incorrect subscription frame")
		return nil
	}
	return request.Subscriptions
}

func acknowledge(t *testing.T, conn *websocket.Conn, subs []Subscription) {
	t.Helper()
	for _, sub := range subs {
		if err := conn.WriteJSON(acknowledgement{Op: "subscribed", RID: "subscribe", Channel: sub.Channel}); err != nil {
			return
		}
	}
}

func waitForClose(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func receiveEvent(t *testing.T, c *Client) Event {
	t.Helper()
	select {
	case event, ok := <-c.Events():
		if !ok {
			t.Fatalf("events closed: %v", c.Err())
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

func waitDone(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("client did not stop")
	}
}

func TestDialSnapshotsUpdatesAndPongs(t *testing.T) {
	pong := make(chan struct{}, 1)
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		// Real servers may interleave snapshots with per-subscription acks.
		acknowledge(t, conn, subs[:1])
		snapshot := testEvent(CryptoPrices, 1)
		snapshot.Snapshot = true
		snapshot.Payload.Data = []PricePoint{snapshot.Payload.PricePoint}
		_ = conn.WriteJSON(snapshot)
		acknowledge(t, conn, subs[1:])
		empty := testEvent(CryptoTWAP, 1)
		empty.Snapshot = true
		empty.Payload.Data = []PricePoint{}
		_ = conn.WriteJSON(empty)
		_ = conn.WriteJSON(testEvent(CryptoTWAP, 2))
		conn.SetPongHandler(func(string) error {
			select {
			case pong <- struct{}{}:
			default:
			}
			return nil
		})
		_ = conn.WriteControl(websocket.PingMessage, []byte("server-ping"), time.Now().Add(time.Second))
		kind, _, err := conn.ReadMessage()
		if err == nil && kind == websocket.TextMessage {
			t.Error("unexpected client text heartbeat")
		}
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	first := receiveEvent(t, c)
	second := receiveEvent(t, c)
	third := receiveEvent(t, c)
	if !first.Snapshot || len(first.Payload.Data) != 1 || !second.Snapshot || len(second.Payload.Data) != 0 {
		t.Fatal("snapshots not preserved")
	}
	if third.Snapshot || third.Seq != 2 || third.ConnectionID != 1 || third.Gap != 0 {
		t.Fatalf("incorrect update metadata: %+v", third)
	}
	select {
	case <-pong:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not answer server ping")
	}
	if err := c.Close(); err != nil || c.Err() != nil {
		t.Fatalf("clean close: %v, terminal=%v", err, c.Err())
	}
}

func TestDialWaitsForAllAcknowledgements(t *testing.T) {
	firstAck := make(chan struct{})
	release := make(chan struct{})
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs[:1])
		close(firstAck)
		select {
		case <-release:
		case <-t.Context().Done():
			return
		}
		acknowledge(t, conn, subs[1:])
		waitForClose(conn)
	})
	type result struct {
		c   *Client
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		c, err := Dial(t.Context(), cfg)
		resultCh <- result{c: c, err: err}
	}()
	<-firstAck
	select {
	case r := <-resultCh:
		if r.c != nil {
			_ = r.c.Close()
		}
		t.Fatal("Dial returned before all subscriptions were accepted")
	default:
	}
	close(release)
	r := <-resultCh
	if r.err != nil {
		t.Fatal(r.err)
	}
	_ = r.c.Close()
}

func TestReconnectAuthenticatesResubscribesAndResetsSequences(t *testing.T) {
	var connections atomic.Int32
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		n := connections.Add(1)
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		event := testEvent(CryptoTWAP, 1)
		event.Snapshot = true
		event.Payload.Data = []PricePoint{event.Payload.PricePoint}
		_ = conn.WriteJSON(event)
		if n == 1 {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseServiceRestart, ""),
				time.Now().Add(time.Second),
			)
			return
		}
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	a, b := receiveEvent(t, c), receiveEvent(t, c)
	if a.ConnectionID != 1 || b.ConnectionID != 2 || b.Seq != 1 || b.Gap != 0 || !b.Snapshot {
		t.Fatalf("bad reconnect metadata: first=%+v second=%+v", a, b)
	}
	select {
	case err := <-c.Errors():
		interruption, ok := errors.AsType[*ConnectionError](err)
		if !ok || !interruption.Reconnecting {
			t.Fatalf("expected recovery notification, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing disconnect notification")
	}
}

func TestTerminalServerCloseCodes(t *testing.T) {
	for _, code := range []int{4001, 4008, 1008} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var count atomic.Int32
			cfg := testConfig()
			cfg.URL = socketServer(t, func(conn *websocket.Conn) {
				count.Add(1)
				subs := acceptSubscriptions(t, conn)
				acknowledge(t, conn, subs)
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(code, "test-secret"),
					time.Now().Add(time.Second),
				)
			})
			c, err := Dial(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			waitDone(t, c)
			closed, ok := errors.AsType[*CloseError](c.Err())
			if !ok || closed.Code != code || count.Load() != 1 {
				t.Fatalf("unexpected terminal error or retry: %v count=%d", c.Err(), count.Load())
			}
			if strings.Contains(c.Err().Error(), "test-secret") {
				t.Fatal("server close text leaked")
			}
		})
	}
}

func TestSlowConsumerIsTerminal(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 1
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		_ = conn.WriteJSON(testEvent(CryptoPrices, 1))
		_ = conn.WriteJSON(testEvent(CryptoPrices, 2))
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitDone(t, c)
	if !errors.Is(c.Err(), ErrSlowConsumer) {
		t.Fatalf("expected explicit data-loss error, got %v", c.Err())
	}
	if got := receiveEvent(t, c); got.Seq != 1 {
		t.Fatal("buffered event lost")
	}
}

func TestServerLossIndicatorsArePreserved(t *testing.T) {
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		_ = conn.WriteJSON(testEvent(CryptoPrices, 1))
		event := testEvent(CryptoPrices, 4)
		event.Dropped = 2
		_ = conn.WriteJSON(event)
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	receiveEvent(t, c)
	event := receiveEvent(t, c)
	if event.Gap != 2 || event.Dropped != 2 {
		t.Fatalf("loss indicators missing: %+v", event)
	}
}

func TestConcurrentCloseAndParentCancellation(t *testing.T) {
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		waitForClose(conn)
	})
	ctx, cancel := context.WithCancel(t.Context())
	c, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { _ = c.Close() })
	}
	wg.Wait()
	waitDone(t, c)
	if c.Err() != nil {
		t.Fatalf("cancellation should be clean: %v", c.Err())
	}
}

func TestCloseInterruptsReconnectHandshake(t *testing.T) {
	reconnecting := make(chan struct{})
	var count atomic.Int32
	cfg := testConfig()
	cfg.HandshakeTimeout = time.Minute
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		if count.Add(1) > 1 {
			close(reconnecting)
			waitForClose(conn) // deliberately never acknowledge auth
			return
		}
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	select {
	case <-reconnecting:
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect")
	}
	closed := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on reconnect authentication")
	}
}

func TestMalformedEventTerminates(t *testing.T) {
	for _, raw := range []string{
		"{", `{"v":2}`, `{"op":"unexpected"}`,
		`{"v":1,"channel":"price.crypto","seq":1,"ts":1,"payload":null}`,
	} {
		t.Run(raw, func(t *testing.T) {
			cfg := testConfig()
			cfg.URL = socketServer(t, func(conn *websocket.Conn) {
				subs := acceptSubscriptions(t, conn)
				acknowledge(t, conn, subs)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(raw))
				waitForClose(conn)
			})
			c, err := Dial(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			waitDone(t, c)
			if !errors.Is(c.Err(), ErrProtocol) {
				t.Fatalf("expected protocol failure, got %v", c.Err())
			}
		})
	}
}

func TestEventJSONRetainsWireFields(t *testing.T) {
	e := testEvent(CryptoTWAP, 1)
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "ConnectionID") || !strings.Contains(string(raw), "full_accuracy_value") {
		t.Fatal("incorrect wire event serialization")
	}
}
