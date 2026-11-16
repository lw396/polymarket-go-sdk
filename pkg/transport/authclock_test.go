package transport

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Illustrative test settings, NOT an exchange acceptance policy.
func testAuthClockPolicy() AuthClockPolicy {
	return AuthClockPolicy{MaxAge: 5 * time.Second, MaxRTT: 500 * time.Millisecond, MaxUncertainty: 750 * time.Millisecond, MaxWallJump: 2 * time.Second, RefreshTimeout: time.Second, SendBudget: 500 * time.Millisecond, MaxDriftPPM: 50}
}

func newTestAuthClock(t *testing.T, fetch AuthClockFetch) *AuthClock {
	t.Helper()
	c, err := NewAuthClock("https://clob.example.invalid", testAuthClockPolicy(), fetch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestAuthClockAdvancesAndExpiresWithoutFetchingOnRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := newTestAuthClock(t, func(context.Context, string) (int64, error) {
			calls.Add(1)
			time.Sleep(260 * time.Millisecond)
			return 1794817200, nil
		})
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockUnavailable) {
			t.Fatalf("cold read=%v", err)
		}
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		first, err := c.Read(t.Context())
		if err != nil || first.Timestamp != 1794817200 || first.RTT != 260*time.Millisecond || first.Uncertainty < 630*time.Millisecond {
			t.Fatalf("reading=%+v err=%v", first, err)
		}
		time.Sleep(time.Second)
		next, err := c.Read(t.Context())
		if err != nil || next.Timestamp != first.Timestamp+1 || next.Uncertainty <= first.Uncertainty || calls.Load() != 1 {
			t.Fatalf("not advancing locally: %+v %v calls=%d", next, err, calls.Load())
		}
		time.Sleep(4 * time.Second)
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockStale) {
			t.Fatalf("expired read=%v", err)
		}
		if calls.Load() != 1 {
			t.Fatal("Read issued a refresh")
		}
	})
}

func TestAuthClockRefreshCannotMoveIssuedTimeBackward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		p := testAuthClockPolicy()
		p.MaxDriftPPM = 0
		c, err := NewAuthClock("https://clob.example.invalid", p, func(context.Context, string) (int64, error) {
			calls++
			if calls == 1 {
				time.Sleep(500 * time.Millisecond)
			}
			return 1794817200, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
		before, err := c.Read(t.Context())
		if err != nil || before.Timestamp != 1794817201 {
			t.Fatal(before, err)
		}
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		after, err := c.Read(t.Context())
		if err != nil || after.Timestamp < before.Timestamp || after.MaxFutureSkew != time.Second {
			t.Fatalf("refresh moved timestamp backward or hid skew: before=%+v after=%+v err=%v", before, after, err)
		}
	})
}

func TestAuthClockServerJumpDoesNotClampOutsideInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		seconds := int64(1794817200)
		c := newTestAuthClock(t, func(context.Context, string) (int64, error) { return seconds, nil })
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Read(t.Context()); err != nil {
			t.Fatal(err)
		}
		seconds -= 100
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockJump) {
			t.Fatalf("out-of-interval timestamp allowed: %v", err)
		}
	})
}

func TestAuthClockTimeDiscontinuities(t *testing.T) {
	for _, tt := range []struct {
		name       string
		wall, mono time.Duration
		during     bool
	}{
		{"wall forward", 3 * time.Second, 0, false}, {"wall backward", -3 * time.Second, 0, false},
		{"monotonic backward", 0, -time.Second, false}, {"jump during refresh", 3 * time.Second, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started, release := make(chan struct{}), make(chan struct{})
				c := newTestAuthClock(t, func(context.Context, string) (int64, error) {
					if tt.during {
						close(started)
						<-release
					}
					return 1794817200, nil
				})
				var wall, mono atomic.Int64
				c.state.now = func() authClockInstant {
					n := time.Now()
					return authClockInstant{mono: n.Add(time.Duration(mono.Load())), wall: n.Add(time.Duration(wall.Load())).Round(0)}
				}
				if tt.during {
					done := make(chan error, 1)
					go func() { done <- c.Refresh(t.Context()) }()
					<-started
					wall.Store(int64(tt.wall))
					close(release)
					if err := <-done; !errors.Is(err, ErrAuthClockJump) {
						t.Fatalf("refresh jump=%v", err)
					}
				} else {
					if err := c.Refresh(t.Context()); err != nil {
						t.Fatal(err)
					}
					wall.Store(int64(tt.wall))
					mono.Store(int64(tt.mono))
					if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockJump) {
						t.Fatalf("read jump=%v", err)
					}
				}
				if _, err := c.Read(t.Context()); err == nil {
					t.Fatal("discontinuity sample resurrected")
				}
			})
		})
	}
}

func TestAuthClockRefreshFailureDoesNotExtendSample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		c := newTestAuthClock(t, func(context.Context, string) (int64, error) {
			calls++
			if calls > 1 {
				return 0, errors.New("opaque failure")
			}
			return 1794817200, nil
		})
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(4 * time.Second)
		if err := c.Refresh(t.Context()); !errors.Is(err, ErrAuthClockSync) {
			t.Fatal(err)
		}
		r, err := c.Read(t.Context())
		if err != nil || r.Age != 4*time.Second {
			t.Fatalf("fresh old sample changed: %+v %v", r, err)
		}
		time.Sleep(time.Second)
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockStale) {
			t.Fatal("failed refresh extended sample")
		}
	})
}

func TestAuthClockDeadlineAndUncooperativeFetcherStayBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		c := newTestAuthClock(t, func(context.Context, string) (int64, error) {
			calls.Add(1)
			close(started)
			<-release
			return 1794817200, nil
		})
		done := make(chan error, 1)
		go func() { done <- c.Refresh(t.Context()) }()
		<-started
		time.Sleep(time.Second)
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline did not release waiter: %v", err)
		}
		for range 10 {
			if err := c.Refresh(t.Context()); !errors.Is(err, ErrAuthClockBusy) {
				t.Fatalf("extra worker admitted: %v", err)
			}
		}
		if calls.Load() != 1 {
			t.Fatal("unbounded callbacks")
		}
		c.Close()
		close(release)
		synctest.Wait()
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockClosed) {
			t.Fatal(err)
		}
	})
}

func TestAuthClockLastWaiterCancellationAllowsLaterRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		var calls atomic.Int32
		c := newTestAuthClock(t, func(ctx context.Context, _ string) (int64, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 1794817200, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.Refresh(ctx) }()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal("cooperative callback did not release flight", err)
		}
	})
}

func TestAuthClockUncertaintyAndArithmeticBounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := testAuthClockPolicy()
		p.MaxDriftPPM = 1_000_000
		p.MaxUncertainty = 700 * time.Millisecond
		c, err := NewAuthClock("https://clob.example.invalid", p, func(context.Context, string) (int64, error) { return 1794817200, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := c.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(201 * time.Millisecond)
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockUncertain) {
			t.Fatal("growing uncertainty accepted", err)
		}
		if c.state.drift(24*time.Hour) != 24*time.Hour || c.state.drift(time.Nanosecond) != time.Nanosecond {
			t.Fatal("drift overflow or rounding")
		}
	})
}

func TestAuthClockConstructionAndZeroValueSafety(t *testing.T) {
	fetch := func(context.Context, string) (int64, error) { t.Fatal("unexpected fetch"); return 0, nil }
	for _, origin := range []string{"", "file:///tmp/time", "https://user:secret@clob.example.invalid", "https://clob.example.invalid/path", "https://clob.example.invalid?secret=x", "https://clob.example.invalid/#x", "https://clob.example.invalid:65536"} {
		if _, err := NewAuthClock(origin, testAuthClockPolicy(), fetch); !errors.Is(err, ErrAuthClockOrigin) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("origin accepted/leaked: %v", err)
		}
	}
	p := testAuthClockPolicy()
	p.MaxAge = 0
	if _, err := NewAuthClock("https://clob.example.invalid", p, fetch); !errors.Is(err, ErrAuthClockPolicy) {
		t.Fatal("missing policy accepted")
	}
	p = testAuthClockPolicy()
	p.MaxDriftPPM = 1_000_001
	if _, err := NewAuthClock("https://clob.example.invalid", p, fetch); !errors.Is(err, ErrAuthClockPolicy) {
		t.Fatal("overflow policy accepted")
	}
	for _, c := range []*AuthClock{nil, {}} {
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockUnavailable) {
			t.Fatal(err)
		}
		if err := c.Refresh(t.Context()); !errors.Is(err, ErrAuthClockUnavailable) {
			t.Fatal(err)
		}
		c.Invalidate()
		c.Close()
	}
	c := newTestAuthClock(t, fetch)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAuthClockInvalidSamples(t *testing.T) {
	for _, tt := range []struct {
		name    string
		seconds int64
		delay   time.Duration
		failure bool
	}{
		{"zero", 0, 0, false}, {"negative", -1, 0, false}, {"overflow", 1 << 62, 0, false}, {"slow", 1794817200, 501 * time.Millisecond, false}, {"opaque failure", 1794817200, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newTestAuthClock(t, func(context.Context, string) (int64, error) {
					time.Sleep(tt.delay)
					if tt.failure {
						return 0, errors.New("SYNTHETIC_SECRET_NEVER_RETURN")
					}
					return tt.seconds, nil
				})
				err := c.Refresh(t.Context())
				if err == nil || strings.Contains(err.Error(), "SYNTHETIC_SECRET") {
					t.Fatalf("invalid sample or unsafe error=%v", err)
				}
				if _, err := c.Read(t.Context()); err == nil {
					t.Fatal("invalid sample became available")
				}
			})
		})
	}
}

func TestAuthClockCloneScopeAndSharedRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		started, release := make(chan struct{}), make(chan struct{})
		c := newTestAuthClock(t, func(ctx context.Context, origin string) (int64, error) {
			if origin != "https://clob.example.invalid" {
				t.Error("bad scoped origin")
			}
			calls.Add(1)
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 1794817200, nil
			}
		})
		clone, err := c.CloneForOrigin("https://CLOB.example.invalid:443/")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.CloneForOrigin("https://other.example.invalid"); !errors.Is(err, ErrAuthClockOrigin) {
			t.Fatal("cross-origin clone shared clock")
		}
		var wg sync.WaitGroup
		results := make(chan error, 20)
		for range 20 {
			wg.Go(func() { results <- clone.Refresh(t.Context()) })
		}
		<-started
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("refresh requests=%d", calls.Load())
		}
		close(release)
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Read(t.Context()); err != nil {
			t.Fatal("clone did not share result")
		}
		clone.Close()
		if _, err := c.Read(t.Context()); !errors.Is(err, ErrAuthClockClosed) {
			t.Fatal("clone lifetime not shared")
		}
	})
}

func TestAuthClockCanceledWaiterDoesNotCancelOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		c := newTestAuthClock(t, func(ctx context.Context, _ string) (int64, error) {
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 1794817200, nil
			}
		})
		ctx, cancel := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- c.Refresh(ctx) }()
		<-started
		go func() { second <- c.Refresh(t.Context()) }()
		synctest.Wait()
		cancel()
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		close(release)
		if err := <-second; err != nil {
			t.Fatal("one waiter canceled shared refresh", err)
		}
	})
}

func TestAuthClockInvalidateRejectsLateResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		c := newTestAuthClock(t, func(context.Context, string) (int64, error) { close(started); <-release; return 1794817200, nil })
		done := make(chan error, 1)
		go func() { done <- c.Refresh(t.Context()) }()
		<-started
		c.Invalidate()
		synctest.Wait()
		if err := <-done; err == nil {
			t.Fatal("invalidation did not release waiter")
		}
		if err := c.Refresh(t.Context()); !errors.Is(err, ErrAuthClockBusy) {
			t.Fatalf("uncooperative callback admitted more work: %v", err)
		}
		close(release)
		synctest.Wait()
		if _, err := c.Read(t.Context()); err == nil {
			t.Fatal("late canceled response resurrected sample")
		}
	})
}
