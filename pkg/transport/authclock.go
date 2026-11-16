package transport

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrAuthClockUnavailable = errors.New("auth clock has no calibration")
	ErrAuthClockPolicy      = errors.New("invalid explicit auth clock policy")
	ErrAuthClockOrigin      = errors.New("invalid or different auth clock origin")
	ErrAuthClockStale       = errors.New("auth clock calibration expired")
	ErrAuthClockUncertain   = errors.New("auth clock uncertainty exceeds policy")
	ErrAuthClockJump        = errors.New("auth clock time discontinuity")
	ErrAuthClockSample      = errors.New("invalid auth clock sample")
	ErrAuthClockSync        = errors.New("auth clock synchronization failed")
	ErrAuthClockClosed      = errors.New("auth clock is closed")
	ErrAuthClockBusy        = errors.New("auth clock canceled refresh has not returned")
	ErrAuthClockInvalidated = errors.New("auth clock calibration invalidated")
)

// AuthClockPolicy has NO production defaults and is NOT the exchange's
// acceptance window. Durations must be positive and at most 24h (an arithmetic
// bound, not an acceptable calibration lifetime). Drift is a caller assumption.
// Quantization alone requires MaxUncertainty >= 500ms. A zero MaxDriftPPM
// explicitly assumes zero drift; 1e6 is the maximum supported rate bound.
type AuthClockPolicy struct {
	MaxAge         time.Duration
	MaxRTT         time.Duration
	MaxUncertainty time.Duration
	MaxWallJump    time.Duration
	RefreshTimeout time.Duration
	SendBudget     time.Duration
	MaxDriftPPM    int64
}

// AuthClockFetch returns Unix SECONDS sampled by the named origin. It must
// honor cancellation/deadlines. The component has no HTTP client, credentials,
// signer or retry behavior; callers must bind this to a trusted public source.
// Samples assume floor-to-second quantization, fresh sampling within the call
// interval, no stale proxy response, and the stated drift bound. None of these
// assumptions, nor this policy's tolerance, is guaranteed by the exchange.
type AuthClockFetch func(context.Context, string) (int64, error)

// AuthClockReading is conditional timing evidence, NOT authentication success.
// Timestamp is seconds; durations include conservative quantization/drift.
// Uncertainty is interval half-width, not the maximum timestamp skew: integer
// flooring and the caller's SendBudget are included in the separate skew bounds.
type AuthClockReading struct {
	Timestamp     int64
	Age           time.Duration
	RTT           time.Duration
	Uncertainty   time.Duration
	MaxPastSkew   time.Duration
	MaxFutureSkew time.Duration
}

// AuthClock is an optional calibration handle with no implicit integration.
// SetAuthClock explicitly attaches it to a transport's server-time branch.
// Construct it with an explicit policy and call Refresh explicitly; Read never
// fetches or spawns. The caller, not transport clones, owns its lifetime.
// Do not copy it; use CloneForOrigin for shared same-origin calibration/lifetime.
// Its zero value and nil receiver are unavailable, not a local-time fallback.
type AuthClock struct{ state *authClockState }

type authClockInstant struct{ mono, wall time.Time }
type authClockSample struct {
	server   time.Time
	received authClockInstant
	rtt      time.Duration
}
type authClockFlight struct {
	ctx        context.Context
	cancel     context.CancelFunc
	started    authClockInstant
	generation uint64
	done       chan struct{}
	waiters    int
	finished   bool
	abandoned  bool
	err        error
	discardErr error
}
type authClockState struct {
	mu            sync.Mutex
	origin        string
	policy        AuthClockPolicy
	fetch         AuthClockFetch
	now           func() authClockInstant
	lifetime      context.Context
	cancel        context.CancelFunc
	closed        bool
	generation    uint64
	sample        *authClockSample
	flight        *authClockFlight
	lastTimestamp int64 // Retained across refresh and invalidation within this lifetime.
}

func NewAuthClock(origin string, policy AuthClockPolicy, fetch AuthClockFetch) (*AuthClock, error) {
	scope, err := authClockOrigin(origin)
	if err != nil {
		return nil, err
	}
	const maxSpan = 24 * time.Hour
	for _, d := range []time.Duration{policy.MaxAge, policy.MaxRTT, policy.MaxUncertainty, policy.MaxWallJump, policy.RefreshTimeout, policy.SendBudget} {
		if d <= 0 || d > maxSpan {
			return nil, ErrAuthClockPolicy
		}
	}
	if policy.MaxUncertainty < time.Second/2 || policy.MaxDriftPPM < 0 || policy.MaxDriftPPM > 1_000_000 || fetch == nil {
		return nil, ErrAuthClockPolicy
	}
	life, cancel := context.WithCancel(context.Background())
	return &AuthClock{state: &authClockState{origin: scope, policy: policy, fetch: fetch, lifetime: life, cancel: cancel, now: func() authClockInstant { n := time.Now(); return authClockInstant{mono: n, wall: n.Round(0)} }}}, nil
}

func authClockOrigin(raw string) (string, error) {
	if len(raw) > 2048 {
		return "", ErrAuthClockOrigin
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" {
		return "", ErrAuthClockOrigin
	}
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port != "" {
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 {
			return "", ErrAuthClockOrigin
		}
	}
	if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return u.Scheme + "://" + host, nil
}

func (c *AuthClock) CloneForOrigin(origin string) (*AuthClock, error) {
	if c == nil || c.state == nil {
		return nil, ErrAuthClockUnavailable
	}
	scope, err := authClockOrigin(origin)
	if err != nil {
		return nil, err
	}
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrAuthClockClosed
	}
	if scope != s.origin {
		return nil, ErrAuthClockOrigin
	}
	return &AuthClock{state: s}, nil
}

// Refresh coalesces concurrent same-clock calls. Each caller may cancel its own
// wait without canceling other waiters. The last departing waiter cancels the
// worker. A callback ignoring cancellation cannot cause an unbounded pile-up:
// subsequent refreshes return Busy until that ONE callback returns. Close does
// not wait for an uncooperative callback, but cancels it and releases waiters.
func (c *AuthClock) Refresh(ctx context.Context) error {
	if c == nil || c.state == nil || ctx == nil {
		return ErrAuthClockUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s := c.state
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrAuthClockClosed
	}
	f := s.flight
	if f != nil && (f.abandoned || f.ctx.Err() != nil) {
		s.mu.Unlock()
		return ErrAuthClockBusy
	}
	if f == nil {
		work, cancel := context.WithTimeout(s.lifetime, s.policy.RefreshTimeout)
		f = &authClockFlight{ctx: work, cancel: cancel, started: s.now(), generation: s.generation, done: make(chan struct{})}
		s.flight = f
		go s.refresh(f)
	}
	f.waiters++
	s.mu.Unlock()
	defer s.leave(f)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.done:
	case <-f.ctx.Done():
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.finished {
		return f.err
	}
	if f.discardErr != nil {
		return f.discardErr
	}
	return f.ctx.Err()
}

func (s *authClockState) leave(f *authClockFlight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f.waiters--
	if f.waiters == 0 && !f.finished {
		f.abandoned = true
		f.discardErr = context.Canceled
		f.cancel()
	}
}

func (s *authClockState) refresh(f *authClockFlight) {
	var seconds int64
	var err error
	if err = f.ctx.Err(); err == nil {
		seconds, err = s.fetch(f.ctx, s.origin)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer f.cancel()
	finished := s.now()
	switch {
	case s.closed:
		f.err = ErrAuthClockClosed
	case f.abandoned || s.generation != f.generation:
		f.err = f.discardErr
		if f.err == nil {
			f.err = ErrAuthClockInvalidated
		}
	case f.ctx.Err() != nil:
		f.err = f.ctx.Err()
	case err != nil:
		f.err = ErrAuthClockSync // Never retain or return raw source errors.
	default:
		sample, e := s.calibrate(f.started, finished, seconds)
		f.err = e
		if e == nil {
			s.sample = sample
		} else if errors.Is(e, ErrAuthClockJump) {
			s.sample = nil
			s.generation++
		}
	}
	f.finished = true
	if s.flight == f {
		s.flight = nil
	}
	close(f.done)
}

// drift rounds up at nanosecond precision without multiplying a whole duration
// by ppm (which could overflow). Callers pass nonnegative bounded durations.
func (s *authClockState) drift(d time.Duration) time.Duration {
	n := int64(d)
	ppm := s.policy.MaxDriftPPM
	return time.Duration((n/1_000_000)*ppm + ((n%1_000_000)*ppm+999_999)/1_000_000)
}

func (s *authClockState) calibrate(start, end authClockInstant, seconds int64) (*authClockSample, error) {
	rtt := end.mono.Sub(start.mono)
	// Restrict sample dates to an explicit civil-time domain; no UnixNano
	// arithmetic or int64 multiplication of externally supplied timestamps.
	if seconds <= 0 || seconds > 253402214400 || rtt < 0 || rtt > s.policy.MaxRTT {
		return nil, ErrAuthClockSample
	}
	wallElapsed := end.wall.Sub(start.wall)
	if wallElapsed < rtt-s.policy.MaxWallJump || wallElapsed > rtt+s.policy.MaxWallJump {
		return nil, ErrAuthClockJump
	}
	span := time.Second + rtt + s.drift(rtt)
	if (span+1)/2 > s.policy.MaxUncertainty {
		return nil, ErrAuthClockUncertain
	}
	return &authClockSample{server: time.Unix(seconds, 0), received: end, rtt: rtt}, nil
}

// Read returns only a cached, locally advanced timestamp or an error. Expiry or
// discontinuity seals off the old sample and any in-flight old-generation reply.
func (c *AuthClock) Read(ctx context.Context) (AuthClockReading, error) {
	if c == nil || c.state == nil || ctx == nil {
		return AuthClockReading{}, ErrAuthClockUnavailable
	}
	if err := ctx.Err(); err != nil {
		return AuthClockReading{}, err
	}
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return AuthClockReading{}, ErrAuthClockClosed
	}
	sample := s.sample
	if sample == nil {
		return AuthClockReading{}, ErrAuthClockUnavailable
	}
	now := s.now()
	age := now.mono.Sub(sample.received.mono)
	if age < 0 {
		s.invalidateLocked(ErrAuthClockJump)
		return AuthClockReading{}, ErrAuthClockJump
	}
	if age >= s.policy.MaxAge {
		s.invalidateLocked(ErrAuthClockStale)
		return AuthClockReading{}, ErrAuthClockStale
	}
	wallElapsed := now.wall.Sub(sample.received.wall)
	if wallElapsed < age-s.policy.MaxWallJump || wallElapsed > age+s.policy.MaxWallJump {
		s.invalidateLocked(ErrAuthClockJump)
		return AuthClockReading{}, ErrAuthClockJump
	}
	margin := s.drift(age)
	low := sample.server.Add(age - margin)
	high := sample.server.Add(time.Second + sample.rtt + s.drift(sample.rtt) + age + margin)
	width := high.Sub(low)
	if (width+1)/2 > s.policy.MaxUncertainty {
		s.invalidateLocked(ErrAuthClockUncertain)
		return AuthClockReading{}, ErrAuthClockUncertain
	}
	stamp := max(low.Add(width/2).Unix(), s.lastTimestamp)
	at := time.Unix(stamp, 0)
	// A fresher, tighter interval may shift its midpoint backward. Preserve
	// issued-time monotonicity only while that value fits the new interval;
	// otherwise refuse instead of silently issuing far-future authentication.
	if at.After(high) {
		s.invalidateLocked(ErrAuthClockJump)
		return AuthClockReading{}, ErrAuthClockJump
	}
	s.lastTimestamp = stamp
	return AuthClockReading{Timestamp: stamp, Age: age, RTT: sample.rtt, Uncertainty: (width + 1) / 2,
		MaxPastSkew: high.Add(s.policy.SendBudget + s.drift(s.policy.SendBudget)).Sub(at), MaxFutureSkew: max(time.Duration(0), at.Sub(low))}, nil
}

func (s *authClockState) invalidateLocked(reason error) {
	s.sample = nil
	s.generation++
	if f := s.flight; f != nil && !f.finished {
		f.abandoned = true
		f.discardErr = reason
		f.cancel()
	}
}

func (c *AuthClock) Invalidate() {
	if c == nil || c.state == nil {
		return
	}
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidateLocked(ErrAuthClockInvalidated)
}

// Close cancels the shared lifetime; closing ANY clone closes all its peers.
// A later process must construct a new clock; there is no persistence facility.
func (c *AuthClock) Close() {
	if c == nil || c.state == nil {
		return
	}
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.invalidateLocked(ErrAuthClockClosed)
	s.cancel()
}
