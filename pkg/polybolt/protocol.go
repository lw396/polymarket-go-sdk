package polybolt

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gorilla/websocket"
)

const maxFrameSize = 64 << 10

type acknowledgement struct {
	Op      string  `json:"op"`
	RID     string  `json:"rid"`
	Code    string  `json:"code"`
	Channel Channel `json:"channel"`
}

type session struct {
	conn       *websocket.Conn
	id         uint64
	sequences  map[Channel]uint64
	subs       []Subscription
	bufferSize int
}

func (s *session) read() (acknowledgement, *Event, error) {
	kind, raw, err := s.conn.ReadMessage()
	if err != nil {
		return acknowledgement{}, nil, safeSocketError(err)
	}
	if kind != websocket.TextMessage {
		return acknowledgement{}, nil, ErrProtocol
	}
	var ack acknowledgement
	if err := json.Unmarshal(raw, &ack); err != nil {
		return ack, nil, ErrProtocol
	}
	if ack.Op == "error" {
		// Only machine codes are surfaced. Never echo untrusted raw messages.
		if !safeCode(ack.Code) {
			return ack, nil, ErrProtocol
		}
		if ack.Channel != "" && ack.Channel != CryptoPrices && ack.Channel != CryptoTWAP {
			return ack, nil, ErrProtocol
		}
		return ack, nil, &RejectedError{Code: ack.Code, Channel: ack.Channel}
	}
	if ack.Op != "" {
		return ack, nil, nil
	}
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return ack, nil, ErrProtocol
	}
	if err := s.validateEvent(&event); err != nil {
		return ack, nil, err
	}
	return ack, &event, nil
}

func safeCode(code string) bool {
	if code == "" || len(code) > 64 {
		return false
	}
	for _, ch := range code {
		if ch != '_' && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
			return false
		}
	}
	return true
}

func (s *session) validateEvent(event *Event) error {
	if event.Version != 1 || event.Seq == 0 || event.Timestamp <= 0 {
		return ErrProtocol
	}
	payload := event.Payload
	matched := false
	for _, sub := range s.subs {
		if sub.Channel == event.Channel && sub.Filter.Symbol == payload.Symbol &&
			sub.Filter.WindowSeconds == payload.WindowSeconds {
			matched = true
			break
		}
	}
	if !matched || payload.Source == "" {
		return ErrProtocol
	}
	if event.Channel == CryptoTWAP && payload.Source != "chainlink" {
		return ErrProtocol
	}
	if event.Snapshot {
		if payload.Data == nil {
			return ErrProtocol
		}
		for _, point := range payload.Data {
			if err := validatePoint(point); err != nil {
				return err
			}
		}
	} else {
		if payload.Data != nil {
			return ErrProtocol
		}
		if err := validatePoint(payload.PricePoint); err != nil {
			return err
		}
	}
	last := s.sequences[event.Channel]
	if event.Seq <= last {
		return fmt.Errorf("%w: non-increasing sequence", ErrProtocol)
	}
	event.Gap = event.Seq - last - 1
	event.ConnectionID = s.id
	s.sequences[event.Channel] = event.Seq
	return nil
}

func validatePoint(point PricePoint) error {
	value, err := point.Decimal()
	if err != nil || point.Timestamp <= 0 {
		return ErrProtocol
	}
	if !value.IsPositive() {
		return ErrProtocol
	}
	return nil
}

func safeSocketError(err error) error {
	if errors.Is(err, websocket.ErrReadLimit) {
		return fmt.Errorf("%w: incoming frame exceeds 64 KiB", ErrProtocol)
	}
	if closeErr, ok := errors.AsType[*websocket.CloseError](err); ok {
		return &CloseError{Code: closeErr.Code}
	}
	return err
}
