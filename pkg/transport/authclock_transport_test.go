package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/auth"
)

type authClockRoundTrip func(*http.Request) (*http.Response, error)

func (f authClockRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func authClockResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}
func authenticatedClockTransport(t *testing.T, rt authClockRoundTrip) *Client {
	t.Helper()
	c := NewClient(&http.Client{Transport: rt}, "https://clob.example.invalid")
	signer, err := auth.NewPrivateKeySigner(strings.Repeat("0", 63)+"1", 137)
	if err != nil {
		t.Fatal("synthetic signer")
	}
	c.SetAuth(signer, &auth.APIKey{Key: "synthetic-key", Secret: "c3ludGhldGlj", Passphrase: "synthetic-pass"})
	c.SetUseServerTime(true)
	return c
}

func TestTransportAuthClockStrictCacheAndClone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		times, posts := 0, 0
		c := authenticatedClockTransport(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/time" {
				times++
				if r.Header.Get(auth.HeaderPolySignature) != "" || r.Header.Get(auth.HeaderPolyAPIKey) != "" {
					t.Fatal("public time carried credentials")
				}
				return authClockResponse(r, 200, strconv.FormatInt(time.Now().Unix(), 10)), nil
			}
			posts++
			var body []byte
			if r.Body != nil {
				body, _ = io.ReadAll(r.Body)
			}
			signature, err := auth.SignHMAC("c3ludGhldGlj", r.Header.Get(auth.HeaderPolyTimestamp)+"POST/order"+strings.ReplaceAll(string(body), "'", "\""))
			if err != nil || signature != r.Header.Get(auth.HeaderPolySignature) {
				t.Fatal("HMAC/body changed")
			}
			return authClockResponse(r, 200, `{}`), nil
		})
		clock, err := c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		defer clock.Close()
		if err := c.SetAuthClock(clock); err != nil {
			t.Fatal(err)
		}
		ctx, collector := WithOrderDiagnostics(t.Context())
		assertNoSend := func(client *Client) {
			t.Helper()
			err := client.Post(ctx, "/order", nil, nil)
			if _, ok := errors.AsType[*AuthClockNotReadyError](err); !ok {
				t.Fatalf("missing local no-send proof: %v", err)
			}
		}
		assertNoSend(c)
		if times != 0 || posts != 0 {
			t.Fatal("cold clock fetched time or posted")
		}
		if err := clock.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		clone := c.Clone()
		for _, client := range []*Client{c, clone} {
			if err := client.Post(ctx, "/order", map[string]string{"test": "exact-body"}, nil); err != nil {
				t.Fatal(err)
			}
		}
		assertNoSend(c.CloneWithBaseURL("https://different.example.invalid"))
		time.Sleep(6 * time.Second)
		assertNoSend(clone)
		if times != 1 || posts != 2 {
			t.Fatal("unusable clock fell back or retried")
		}
		snapshot := collector.Finish()
		if len(snapshot.AuthTime) != 5 || snapshot.AuthTime[0].Source != "clock_unavailable" || snapshot.AuthTime[1].Source != "calibrated" || snapshot.AuthTime[4].Source != "clock_unavailable" {
			t.Fatal("missing clock evidence")
		}
	})
}

func TestTransportAuthClock401NeverReplays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		times, posts := 0, 0
		c := authenticatedClockTransport(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/time" {
				times++
				return authClockResponse(r, 200, strconv.FormatInt(time.Now().Unix(), 10)), nil
			}
			posts++
			return authClockResponse(r, 401, `{"error":"unauthorized"}`), nil
		})
		clock, err := c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		defer clock.Close()
		if err := clock.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := c.SetAuthClock(clock); err != nil {
			t.Fatal(err)
		}
		if err := c.Post(t.Context(), "/order", nil, nil); err == nil {
			t.Fatal("401 ignored")
		}
		if times != 1 || posts != 1 {
			t.Fatal("authentication rejection triggered refresh or order replay")
		}
	})
}

func TestTransportAuthClockFailureAfterPriorAttemptRemainsUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		times, posts := 0, 0
		var clock *AuthClock
		c := authenticatedClockTransport(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/time" {
				times++
				return authClockResponse(r, 200, strconv.FormatInt(time.Now().Unix(), 10)), nil
			}
			posts++
			clock.Invalidate()
			return nil, fmt.Errorf("synthetic outcome unknown after order attempt")
		})
		var err error
		clock, err = c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		defer clock.Close()
		if err := clock.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := c.SetAuthClock(clock); err != nil {
			t.Fatal(err)
		}
		err = c.Post(t.Context(), "/order", nil, nil)
		if err == nil {
			t.Fatal("unknown attempt ignored")
		}
		if _, ok := errors.AsType[*AuthClockNotReadyError](err); ok {
			t.Fatal("earlier order attempt incorrectly certified not sent")
		}
		if times != 1 || posts != 1 {
			t.Fatal("clock failure fetched time or replayed order")
		}
	})
}

func TestTransportAuthClockSourceRejectsRedirectOrOversize(t *testing.T) {
	for _, kind := range []string{"redirect", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				c := NewClient(&http.Client{Transport: authClockRoundTrip(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Host != "clob.example.invalid" {
						t.Fatal("followed redirect")
					}
					body := strconv.FormatInt(time.Now().Unix(), 10)
					status := 200
					if kind == "oversize" {
						body += strings.Repeat(" ", 65<<10)
					} else {
						status = 302
					}
					resp := authClockResponse(r, status, body)
					resp.Header.Set("Location", "https://forbidden.invalid/")
					return resp, nil
				})}, "https://clob.example.invalid")
				clock, err := c.NewServerAuthClock(testAuthClockPolicy())
				if err != nil {
					t.Fatal(err)
				}
				defer clock.Close()
				if err := clock.Refresh(t.Context()); err == nil {
					t.Fatal("invalid source accepted")
				}
				if calls != 1 {
					t.Fatal("calibration retried or redirected")
				}
			})
		})
	}
}

func TestTransportAuthClockLocalModeAndCustomAuthRemainUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		times, posts := 0, 0
		c := authenticatedClockTransport(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/time" {
				times++
				return authClockResponse(r, 200, strconv.FormatInt(time.Now().Unix(), 10)), nil
			}
			posts++
			return authClockResponse(r, 200, `{}`), nil
		})
		clock, err := c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		defer clock.Close()
		if err := c.SetAuthClock(clock); err != nil {
			t.Fatal(err)
		}
		c.SetUseServerTime(false)
		ctx, collector := WithOrderDiagnostics(t.Context())
		if err := c.Post(ctx, "/order", nil, nil); err != nil {
			t.Fatal(err)
		}
		c.SetUseServerTime(true)
		if err := c.CallWithHeaders(ctx, "POST", "/order", nil, nil, nil, map[string]string{auth.HeaderPolySignature: "synthetic-pre-signed"}); err != nil {
			t.Fatal(err)
		}
		snapshot := collector.Finish()
		if times != 0 || posts != 2 || len(snapshot.AuthTime) != 1 || snapshot.AuthTime[0].Source != "local" {
			t.Fatal("opt-in changed local mode or pre-signed header bypass")
		}
	})
}

func TestTransportAuthClockPublicSourceDoesNotCarryCookieJar(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		jar, _ := cookiejar.New(nil)
		origin, _ := url.Parse("https://clob.example.invalid")
		jar.SetCookies(origin, []*http.Cookie{{Name: "synthetic_session", Value: "not-for-public-source"}})
		client := &http.Client{Jar: jar, Transport: authClockRoundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Cookie") != "" {
				return authClockResponse(r, 403, `{}`), nil
			}
			return authClockResponse(r, 200, strconv.FormatInt(time.Now().Unix(), 10)), nil
		})}
		c := NewClient(client, origin.String())
		clock, err := c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		defer clock.Close()
		if err := clock.Refresh(t.Context()); err != nil {
			t.Fatal("public calibration inherited session cookies")
		}
		if client.Jar != jar || len(client.Jar.Cookies(origin)) != 1 {
			t.Fatal("original client jar was mutated")
		}
	})
}

func TestTransportAuthClockClosedCloneCannotFallBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := authenticatedClockTransport(t, func(*http.Request) (*http.Response, error) {
			t.Fatal("closed clone attempted HTTP fallback")
			return nil, context.Canceled
		})
		clock, err := c.NewServerAuthClock(testAuthClockPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SetAuthClock(clock); err != nil {
			t.Fatal(err)
		}
		clock.Close()
		err = c.Clone().Post(t.Context(), "/order", nil, nil)
		if _, ok := errors.AsType[*AuthClockNotReadyError](err); !ok {
			t.Fatal("closed clone lost strict no-send behavior")
		}
	})
}

func TestTransportAuthClockBindingRejectsOriginMismatch(t *testing.T) {
	c := NewClient(nil, "https://other.example.invalid")
	clock := newTestAuthClock(t, func(context.Context, string) (int64, error) { return 1, nil })
	if err := c.SetAuthClock(clock); err == nil {
		t.Fatal("cross-origin bind allowed")
	}
}
