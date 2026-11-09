package polybolt

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Client owns one socket and a fixed set of subscriptions. Use Dial to create it.
// Events and Errors each have one consumer. Close and Err are concurrency-safe.
type Client struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	events chan Event
	errors chan error
	done   chan struct{}

	mu       sync.Mutex
	conn     *websocket.Conn
	terminal error
}

// Dial waits for authentication and confirmation of every subscription. It does
// not return a partially subscribed client. ctx owns the client's entire lifetime.
// Initial connection failures are returned directly, not retried.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s, pending, err := dialSession(ctx, cfg, 1)
	if err != nil {
		cancel()
		return nil, err
	}
	c := &Client{
		cfg: cfg, ctx: ctx, cancel: cancel, conn: s.conn,
		events: make(chan Event, cfg.BufferSize), errors: make(chan error, 1),
		done: make(chan struct{}),
	}
	stop := context.AfterFunc(ctx, c.closeSocket)
	go func() {
		defer stop()
		c.run(s, pending)
	}()
	return c, nil
}

// Events delivers snapshots and live updates in receive order. It closes when
// recovery fails, ctx is canceled or Close is called. A full buffer is terminal.
func (c *Client) Events() <-chan Event { return c.events }

// Errors reports connection interruptions, including recoverable ones. Only the
// latest unread notification is retained. Err always preserves the terminal error.
func (c *Client) Errors() <-chan error { return c.errors }

// Done closes after the reader exits and both output channels are closed.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err returns the terminal failure, or nil while active or after clean shutdown.
// Buffered Events may precede this failure; inspect Err after draining Events.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminal
}

// Close cancels dialing/backoff, closes the socket and waits for all client work.
// It is idempotent. It does not discard already-buffered events.
func (c *Client) Close() error {
	c.cancel()
	c.closeSocket()
	<-c.done
	return nil
}

func (c *Client) closeSocket() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Client) run(s *session, pending []Event) {
	defer func() {
		c.cancel()
		c.closeSocket()
		close(c.events)
		close(c.errors)
		close(c.done)
	}()
	for {
		err := c.consume(s, pending)
		_ = s.conn.Close()
		if c.ctx.Err() != nil {
			return
		}
		reconnect := c.cfg.Reconnect && retryable(err)
		c.notify(&ConnectionError{ConnectionID: s.id, Reconnecting: reconnect, Err: err})
		if !reconnect {
			c.fail(err)
			return
		}
		s, pending, err = c.recover(s.id+1, err)
		if err != nil {
			if c.ctx.Err() == nil {
				c.fail(err)
			}
			return
		}
	}
}

func (c *Client) consume(s *session, pending []Event) error {
	for _, event := range pending {
		if err := c.deliver(event); err != nil {
			return err
		}
	}
	for {
		ack, event, err := s.read()
		if err != nil {
			return err
		}
		if event == nil {
			if ack.Op != "pong" {
				return ErrProtocol
			}
			continue
		}
		if err := c.deliver(*event); err != nil {
			return err
		}
	}
}

func (c *Client) deliver(event Event) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case c.events <- event:
		return nil
	default:
		return ErrSlowConsumer
	}
}

func (c *Client) recover(id uint64, cause error) (*session, []Event, error) {
	for attempt := 0; c.cfg.MaxReconnectAttempts == -1 || attempt < c.cfg.MaxReconnectAttempts; attempt++ {
		delay := c.cfg.ReconnectDelay
		if closed, ok := errors.AsType[*CloseError](cause); ok && closed.Code == 4003 {
			delay = time.Duration(rand.Int64N(int64(10 * time.Second)))
		}
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return nil, nil, c.ctx.Err()
		case <-timer.C:
		}
		s, pending, err := dialSession(c.ctx, c.cfg, id)
		if err != nil {
			cause = err
			if !retryable(err) {
				return nil, nil, err
			}
			continue
		}
		c.mu.Lock()
		c.conn = s.conn
		c.mu.Unlock()
		if c.ctx.Err() != nil {
			_ = s.conn.Close()
			return nil, nil, c.ctx.Err()
		}
		return s, pending, nil
	}
	return nil, nil, cause
}

func (c *Client) notify(err error) {
	select {
	case c.errors <- err:
		return
	default:
	}
	select {
	case <-c.errors:
	default:
	}
	c.errors <- err
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	c.terminal = err
	c.mu.Unlock()
	c.notify(err)
}
