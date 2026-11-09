package polybolt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func dialSession(ctx context.Context, cfg Config, id uint64) (*session, []Event, error) {
	handshake, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
	defer cancel()
	conn, resp, err := cfg.Dialer.DialContext(handshake, cfg.URL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if handshake.Err() != nil {
			return nil, nil, handshake.Err()
		}
		return nil, nil, err
	}
	stop := context.AfterFunc(handshake, func() { _ = conn.Close() })
	s := &session{
		conn: conn, id: id, sequences: make(map[Channel]uint64),
		subs: cfg.Subscriptions, bufferSize: cfg.BufferSize,
	}
	conn.SetReadLimit(maxFrameSize)
	deadline, _ := handshake.Deadline()
	_ = conn.SetReadDeadline(deadline)
	_ = conn.SetWriteDeadline(deadline)

	pending, err := s.authenticateAndSubscribe(cfg)
	stopped := stop()
	if handshake.Err() != nil {
		err = handshake.Err()
	}
	if !stopped && err == nil {
		err = context.DeadlineExceeded
	}
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	_ = conn.SetWriteDeadline(time.Time{})
	_ = conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
	// Control frames are handled by the reader; no RTDS text PING loop.
	conn.SetPingHandler(func(data string) error {
		if err := conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)); err != nil {
			return err
		}
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	return s, pending, nil
}

func (s *session) authenticateAndSubscribe(cfg Config) ([]Event, error) {
	authFrame := struct {
		Op   string `json:"op"`
		RID  string `json:"rid"`
		Auth struct {
			APIKey     string `json:"apiKey"`
			Secret     string `json:"secret"`
			Passphrase string `json:"passphrase"`
		} `json:"auth"`
	}{Op: "auth", RID: "auth"}
	authFrame.Auth.APIKey = cfg.Credentials.Key
	authFrame.Auth.Secret = cfg.Credentials.Secret
	authFrame.Auth.Passphrase = cfg.Credentials.Passphrase
	if err := s.write(authFrame); err != nil {
		return nil, safeSocketError(err)
	}
	for {
		ack, event, err := s.read()
		if err != nil {
			return nil, err
		}
		if ack.Op == "pong" {
			continue
		}
		if event != nil || ack.Op != "authed" || ack.RID != "auth" {
			return nil, ErrProtocol
		}
		break
	}

	request := struct {
		Op            string         `json:"op"`
		RID           string         `json:"rid"`
		Subscriptions []Subscription `json:"subscriptions"`
	}{Op: "subscribe", RID: "subscribe", Subscriptions: cfg.Subscriptions}
	if err := s.write(request); err != nil {
		return nil, safeSocketError(err)
	}
	remaining := make(map[Channel]int)
	for _, sub := range cfg.Subscriptions {
		remaining[sub.Channel]++
	}
	pending := make([]Event, 0)
	for count := 0; count < len(cfg.Subscriptions); {
		ack, event, err := s.read()
		if err != nil {
			return nil, err
		}
		if event != nil {
			if len(pending) == s.bufferSize {
				return nil, ErrSlowConsumer
			}
			pending = append(pending, *event)
			continue
		}
		if ack.Op == "pong" {
			continue
		}
		valid := ack.Op == "subscribed" && ack.RID == "subscribe" && remaining[ack.Channel] > 0
		if !valid {
			return nil, ErrProtocol
		}
		remaining[ack.Channel]--
		count++
	}
	return pending, nil
}

func (s *session) write(frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return ErrProtocol
	}
	if len(raw) > maxFrameSize {
		return fmt.Errorf("%w: outgoing frame exceeds 64 KiB", ErrProtocol)
	}
	return s.conn.WriteMessage(websocket.TextMessage, raw)
}

func retryable(err error) bool {
	if errors.Is(err, ErrProtocol) || errors.Is(err, ErrSlowConsumer) {
		return false
	}
	if rejection, ok := errors.AsType[*RejectedError](err); ok {
		return rejection.Code == "auth_unavailable"
	}
	if closed, ok := errors.AsType[*CloseError](err); ok {
		switch closed.Code {
		case 4001, 4008, websocket.ClosePolicyViolation:
			return false
		}
	}
	return true
}
