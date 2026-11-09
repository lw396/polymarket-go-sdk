package polybolt

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandshakeRejectionsAndInvalidAcknowledgements(t *testing.T) {
	tests := []struct {
		name       string
		frame      string
		afterAuth  bool
		rejectCode string
	}{
		{name: "auth rejected", frame: `{"op":"error","code":"auth_invalid","rid":"auth"}`, rejectCode: "auth_invalid"},
		{name: "auth unavailable", frame: `{"op":"error","code":"auth_unavailable","rid":"auth"}`,
			rejectCode: "auth_unavailable"},
		{name: "wrong auth RID", frame: `{"op":"authed","rid":"other"}`},
		{name: "data before auth", frame: `{"v":1,"channel":"price.crypto","seq":1,"ts":1}`},
		{name: "malformed auth ack", frame: "not-json"},
		{name: "unsafe code", frame: `{"op":"error","code":"test-secret!","message":"test-secret"}`},
		{name: "unsafe channel", frame: `{"op":"error","code":"bad_filter","channel":"test-secret"}`},
		{name: "bad filter", afterAuth: true,
			frame:      `{"op":"error","code":"bad_filter","channel":"price.crypto","rid":"subscribe"}`,
			rejectCode: "bad_filter"},
		{name: "wrong sub RID", afterAuth: true,
			frame: `{"op":"subscribed","channel":"price.crypto","rid":"wrong"}`},
		{name: "unexpected sub channel", afterAuth: true,
			frame: `{"op":"subscribed","channel":"price.equity","rid":"subscribe"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.URL = socketServer(t, func(conn *websocket.Conn) {
				if tt.afterAuth {
					acceptSubscriptions(t, conn)
				} else {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(tt.frame))
				waitForClose(conn)
			})
			c, err := Dial(t.Context(), cfg)
			if c != nil {
				_ = c.Close()
				t.Fatal("returned partially subscribed client")
			}
			if tt.rejectCode != "" {
				rejected, ok := errors.AsType[*RejectedError](err)
				if !ok || rejected.Code != tt.rejectCode {
					t.Fatalf("expected rejection %q, got %v", tt.rejectCode, err)
				}
			} else if !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected protocol error, got %v", err)
			}
			if strings.Contains(err.Error(), "test-secret") {
				t.Fatal("error leaked server-controlled free text")
			}
		})
	}
}

func TestHandshakeTimeoutAndCancellation(t *testing.T) {
	for _, phase := range []string{"auth", "subscribe"} {
		t.Run(phase, func(t *testing.T) {
			cfg := testConfig()
			cfg.HandshakeTimeout = 40 * time.Millisecond
			cfg.URL = socketServer(t, func(conn *websocket.Conn) {
				if phase == "subscribe" {
					acceptSubscriptions(t, conn)
				}
				waitForClose(conn)
			})
			start := time.Now()
			c, err := Dial(t.Context(), cfg)
			if c != nil {
				_ = c.Close()
				t.Fatal("returned client without acknowledgements")
			}
			if err == nil || time.Since(start) > time.Second {
				t.Fatalf("handshake did not time out promptly: %v", err)
			}
		})
	}
	t.Run("caller canceled", func(t *testing.T) {
		cfg := testConfig()
		cfg.URL = socketServer(t, waitForClose)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := Dial(ctx, cfg); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	})
}

func TestFailedUpgradeClosesResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	if c, err := Dial(t.Context(), cfg); err == nil || c != nil {
		t.Fatal("expected failed upgrade")
	}
}

func TestHandshakeBufferOverflow(t *testing.T) {
	cfg := testConfig()
	cfg.BufferSize = 1
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		acceptSubscriptions(t, conn)
		_ = conn.WriteJSON(testEvent(CryptoPrices, 1))
		_ = conn.WriteJSON(testEvent(CryptoPrices, 2))
		waitForClose(conn)
	})
	if c, err := Dial(t.Context(), cfg); c != nil || !errors.Is(err, ErrSlowConsumer) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("expected handshake overflow, got %v", err)
	}
}

func TestReconnectAttemptLimit(t *testing.T) {
	var count atomic.Int32
	cfg := testConfig()
	cfg.MaxReconnectAttempts = 2
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		if count.Add(1) == 1 {
			subs := acceptSubscriptions(t, conn)
			acknowledge(t, conn, subs)
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON(acknowledgement{Op: "error", Code: "auth_unavailable", RID: "auth"})
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitDone(t, c)
	rejected, ok := errors.AsType[*RejectedError](c.Err())
	if !ok || rejected.Code != "auth_unavailable" || count.Load() != 3 {
		t.Fatalf("retry limit not respected: count=%d err=%v", count.Load(), c.Err())
	}
}

func TestAuthenticationRejectionDuringRecoveryIsTerminal(t *testing.T) {
	var count atomic.Int32
	cfg := testConfig()
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		if count.Add(1) == 1 {
			subs := acceptSubscriptions(t, conn)
			acknowledge(t, conn, subs)
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON(acknowledgement{Op: "error", Code: "auth_invalid", RID: "auth"})
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitDone(t, c)
	rejected, ok := errors.AsType[*RejectedError](c.Err())
	if !ok || rejected.Code != "auth_invalid" || count.Load() != 2 {
		t.Fatalf("retried permanent authentication failure: %v", c.Err())
	}
}

func TestCloseInterruptsReconnectBackoff(t *testing.T) {
	cfg := testConfig()
	cfg.ReconnectDelay = time.Hour
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	select {
	case <-c.Errors():
	case <-time.After(3 * time.Second):
		t.Fatal("no disconnect notification")
	}
	start := time.Now()
	_ = c.Close()
	if time.Since(start) > time.Second {
		t.Fatal("Close did not interrupt backoff")
	}
}

func TestReconnectDisabled(t *testing.T) {
	var count atomic.Int32
	cfg := testConfig()
	cfg.Reconnect = false
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		count.Add(1)
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	waitDone(t, c)
	if c.Err() == nil || count.Load() != 1 {
		t.Fatal("unexpected recovery with reconnect disabled")
	}
}

func TestSameChannelBatchAcknowledgements(t *testing.T) {
	cfg := testConfig()
	cfg.Subscriptions[1] = Subscription{Channel: CryptoPrices, Filter: Filter{Symbol: "ethusd"}}
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		_ = conn.WriteJSON(testEvent(CryptoPrices, 1))
		e := testEvent(CryptoPrices, 2)
		e.Payload.Symbol = "ethusd"
		_ = conn.WriteJSON(e)
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	receiveEvent(t, c)
	if e := receiveEvent(t, c); e.Payload.Symbol != "ethusd" || e.Gap != 0 {
		t.Fatal("same-channel batch was not accepted")
	}
}

func TestFrameSizeAndTypeLimits(t *testing.T) {
	for _, kind := range []int{websocket.TextMessage, websocket.BinaryMessage} {
		t.Run(map[int]string{websocket.TextMessage: "oversized", websocket.BinaryMessage: "binary"}[kind],
			func(t *testing.T) {
				cfg := testConfig()
				cfg.URL = socketServer(t, func(conn *websocket.Conn) {
					subs := acceptSubscriptions(t, conn)
					acknowledge(t, conn, subs)
					_ = conn.WriteMessage(kind, []byte(strings.Repeat("x", maxFrameSize+1)))
					waitForClose(conn)
				})
				c, err := Dial(t.Context(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				waitDone(t, c)
				if !errors.Is(c.Err(), ErrProtocol) {
					t.Fatalf("expected protocol error, got %v", c.Err())
				}
			})
	}
	t.Run("oversized auth", func(t *testing.T) {
		cfg := testConfig()
		cfg.Credentials.Secret = strings.Repeat("x", maxFrameSize)
		cfg.URL = socketServer(t, waitForClose)
		if _, err := Dial(t.Context(), cfg); !errors.Is(err, ErrProtocol) {
			t.Fatalf("expected local frame limit, got %v", err)
		}
	})
	t.Run("oversized subscription batch", func(t *testing.T) {
		cfg := testConfig()
		cfg.Subscriptions[0].Filter.Symbol = strings.Repeat("x", maxFrameSize) + "usd"
		cfg.URL = socketServer(t, func(conn *websocket.Conn) {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			_ = conn.WriteJSON(acknowledgement{Op: "authed", RID: "auth"})
			waitForClose(conn)
		})
		if _, err := Dial(t.Context(), cfg); !errors.Is(err, ErrProtocol) {
			t.Fatalf("expected local subscription frame limit, got %v", err)
		}
	})
}

func TestReadTimeoutRecovers(t *testing.T) {
	var count atomic.Int32
	cfg := testConfig()
	cfg.ReadTimeout = 40 * time.Millisecond
	cfg.URL = socketServer(t, func(conn *websocket.Conn) {
		n := count.Add(1)
		subs := acceptSubscriptions(t, conn)
		acknowledge(t, conn, subs)
		if n > 1 {
			_ = conn.WriteJSON(testEvent(CryptoPrices, 1))
		}
		waitForClose(conn)
	})
	c, err := Dial(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if e := receiveEvent(t, c); e.ConnectionID != 2 {
		t.Fatal("read timeout did not recover")
	}
}
