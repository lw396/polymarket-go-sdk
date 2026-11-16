package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// AuthClockNotReadyError certifies this Call made no HTTP request attempts.
// It is never returned after an earlier attempt whose outcome may be unknown.
// The cause is private so transport owns the no-send certification.
type AuthClockNotReadyError struct{ cause error }

func (e *AuthClockNotReadyError) Error() string {
	return "calibrated authentication clock unavailable before request submission"
}

func (e *AuthClockNotReadyError) Unwrap() error {
	return e.cause
}

// SetAuthClock installs a strict same-origin calibrated server clock. Configure
// before concurrent use, like SetAuth. It only applies when UseServerTime is
// enabled; disabling server time preserves the legacy local-clock behavior.
// An unusable configured clock fails closed, without a live /time fallback.
// A nil clock explicitly restores legacy SDK behavior. Caller owns Refresh and
// Close; requests do not refresh calibration or replay on auth rejection.
func (c *Client) SetAuthClock(clock *AuthClock) error {
	if c == nil {
		return ErrAuthClockUnavailable
	}
	if clock == nil {
		c.authClock = nil
		c.authClockConfigured = false
		return nil
	}
	shared, err := clock.CloneForOrigin(c.baseURL)
	if err != nil {
		return err
	}
	c.authClock = shared
	c.authClockConfigured = true
	return nil
}

// NewServerAuthClock constructs, but neither attaches nor starts, a caller-owned
// clock with an explicit policy. Its public source shares the HTTP connection
// pool, never authentication, and rejects redirects, non-200s and oversized or
// non-integer responses. Refresh has exactly one application HTTP attempt.
// A standard http.Client is required so redirect policy cannot be hidden by a
// custom Doer; its RoundTripper remains a caller trust assumption.
func (c *Client) NewServerAuthClock(policy AuthClockPolicy) (*AuthClock, error) {
	if c == nil {
		return nil, ErrAuthClockUnavailable
	}
	httpClient, ok := c.httpClient.(*http.Client)
	if !ok || httpClient == nil {
		return nil, ErrAuthClockUnavailable
	}
	public := *httpClient
	public.Jar = nil // Public calibration must not inherit account session cookies.
	public.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	origin, userAgent := c.baseURL, c.userAgent
	return NewAuthClock(origin, policy, func(ctx context.Context, _ string) (int64, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/time", nil)
		if err != nil {
			return 0, ErrAuthClockSync
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Cache-Control", "no-cache")
		response, err := public.Do(req)
		if err != nil {
			return 0, ErrAuthClockSync
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return 0, ErrAuthClockSync
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
		if err != nil || len(body) > 64<<10 {
			return 0, ErrAuthClockSync
		}
		var seconds int64
		if json.Unmarshal(body, &seconds) != nil || seconds <= 0 {
			return 0, ErrAuthClockSync
		}
		return seconds, nil
	})
}

func (c *Client) authTimestamp(ctx context.Context) (int64, error) {
	if !c.useServerTime {
		recordOrderAuthTime(ctx, "local", nil)
		return time.Now().Unix(), nil
	}
	if c.authClockConfigured {
		reading, err := c.authClock.Read(ctx)
		if err == nil {
			recordOrderAuthTime(ctx, "calibrated", &reading)
			return reading.Timestamp, nil
		}
		recordOrderAuthTime(ctx, "clock_unavailable", nil)
		return 0, err
	}
	timestamp, err := c.serverTime(ctx)
	if err != nil {
		recordOrderAuthTime(ctx, "server_time_failed", nil)
		return 0, err
	}
	recordOrderAuthTime(ctx, "server_time", nil)
	return timestamp, nil
}
