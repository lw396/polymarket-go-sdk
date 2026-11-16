package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// SetAuthClock installs an optional same-origin server-time overlay. Configure
// before concurrent use, like SetAuth. It only applies when UseServerTime is
// enabled; disabling server time preserves the legacy local-clock behavior.
// A nil clock removes the overlay. Caller owns Refresh and Close; requests do
// not refresh calibration or replay on authentication rejection.
func (c *Client) SetAuthClock(clock *AuthClock) error {
	if c == nil {
		return ErrAuthClockUnavailable
	}
	if clock == nil {
		c.authClock = nil
		return nil
	}
	shared, err := clock.CloneForOrigin(c.baseURL)
	if err != nil {
		return err
	}
	c.authClock = shared
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
	source := "server_time"
	if c.authClock != nil {
		reading, err := c.authClock.Read(ctx)
		if err == nil {
			recordOrderAuthTime(ctx, "calibrated", &reading)
			return reading.Timestamp, nil
		}
		source = "server_time_fallback"
	}
	timestamp, err := c.serverTime(ctx)
	if err != nil {
		recordOrderAuthTime(ctx, "server_time_failed", nil)
		return 0, err
	}
	recordOrderAuthTime(ctx, source, nil)
	return timestamp, nil
}
