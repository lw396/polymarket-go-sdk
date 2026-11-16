package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/types"
)

// MockDoer implements Doer for testing
type MockDoer struct {
	DoFunc func(req *http.Request) (*http.Response, error)
	calls  []*http.Request
}

func (m *MockDoer) Do(req *http.Request) (*http.Response, error) {
	m.calls = append(m.calls, req)
	return m.DoFunc(req)
}

func TestClientErrorRetainsRawResponseAndActualRequestCount(t *testing.T) {
	for _, tt := range []struct {
		name, body   string
		firstFailure bool
	}{
		{"raw fallback", `{"error":"no match"}`, false},
		{"structured message", `{"message":"inner","success":true,"orderID":"other"}`, false},
		{"retry evidence", `{"error":"no match"}`, true},
		{"untrusted metadata ignored", `{"message":"bad request","status":200,"path":"/forged","ResponseBody":"forged","RequestAttempts":1}`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				requests := 0
				mock := &MockDoer{DoFunc: func(req *http.Request) (*http.Response, error) {
					requests++
					status := http.StatusBadRequest
					if tt.firstFailure && requests == 1 {
						status = http.StatusInternalServerError
					}
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(tt.body)), Request: req}, nil
				}}
				client := NewClient(mock, "http://synthetic.invalid")
				err := client.Call(t.Context(), http.MethodPost, "/order", nil, nil, nil, nil)
				response, ok := errors.AsType[*types.Error](err)
				if !ok || response.ResponseBody != tt.body || response.RequestAttempts != requests || response.Status != 400 || response.Path != "/order" || response.ResponseRequestMatched {
					t.Fatalf("raw response/attempt provenance lost: %+v error=%v", response, err)
				}
				if (requests == 2) != tt.firstFailure {
					t.Fatalf("request count=%d firstFailure=%v", requests, tt.firstFailure)
				}
				encoded, err := json.Marshal(response)
				if err != nil || strings.Contains(string(encoded), "ResponseBody") || strings.Contains(string(encoded), "RequestAttempts") || strings.Contains(string(encoded), "ResponseRequestMatched") {
					t.Fatal("transport-only evidence was serialized")
				}
			})
		})
	}
}

func TestResponseProvenanceAllowsDeadlineCloneButNotRedirectOrReplay(t *testing.T) {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://synthetic.invalid/order", nil)
	if err != nil {
		t.Fatal(err)
	}
	clone := request.Clone(t.Context())
	sameURLRedirect := request.Clone(t.Context())
	sameURLRedirect.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
	redirect := request.Clone(t.Context())
	redirect.URL.Path = "/other"
	methodChanged := request.Clone(t.Context())
	methodChanged.Method = http.MethodGet
	for _, tt := range []struct {
		name            string
		responseRequest *http.Request
		writes          int32
		want            bool
	}{
		{"direct doer", request, 0, true},
		{"deadline clone", clone, 1, true},
		{"same URL redirect with pending trace callback", sameURLRedirect, 1, false},
		{"unproven clone", clone, 0, false},
		{"replayed same request", request, 2, false},
		{"replayed clone", clone, 2, false},
		{"redirected URL", redirect, 1, false},
		{"redirected method", methodChanged, 1, false},
		{"missing provenance", nil, 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := responseMatchesRequest(request, &http.Response{Request: tt.responseRequest}, tt.writes)
			if got != tt.want {
				t.Fatalf("matched=%v want=%v", got, tt.want)
			}
		})
	}
}

type proofRoundTripper struct{}

func (proofRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("provenance test must not execute HTTP")
}

func TestOrderResponseProofExcludesOpaqueReplayPolicies(t *testing.T) {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://synthetic.invalid/order", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		doer Doer
		want bool
	}{
		{"standard default transport", &http.Client{Timeout: 30 * time.Second}, true},
		{"standard explicit transport", &http.Client{Transport: &http.Transport{}}, true},
		{"opaque doer", &MockDoer{}, false},
		{"opaque roundtripper", &http.Client{Transport: proofRoundTripper{}}, false},
		{"custom redirect hook", &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if standardNonReplayOrderClient(tt.doer, request) != tt.want {
				t.Fatal("opaque client replay/redirect policy was trusted")
			}
		})
	}
	for _, header := range []string{"Idempotency-Key", "X-Idempotency-Key"} {
		clone := request.Clone(t.Context())
		clone.Header[header] = nil // Header presence, not value, opts into replay.
		if standardNonReplayOrderClient(&http.Client{}, clone) {
			t.Fatal("idempotency opt-in was trusted as a single wire request")
		}
	}
}

func TestClient_Call_Retry(t *testing.T) {
	t.Run("Success on first try", func(t *testing.T) {
		attempts := 0
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				attempts++
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				}, nil
			},
		}

		client := NewClient(mock, "http://example.com")
		err := client.Call(context.Background(), "GET", "/test", nil, nil, nil, nil)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("Retry on 429 then success", func(t *testing.T) {
		attempts := 0
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				attempts++
				if attempts == 1 {
					return &http.Response{
						StatusCode: 429,
						Body:       io.NopCloser(strings.NewReader(`{"error":"too many requests"}`)),
					}, nil
				}
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				}, nil
			},
		}

		client := NewClient(mock, "http://example.com")
		err := client.Call(context.Background(), "GET", "/test", nil, nil, nil, nil)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != 2 {
			t.Errorf("expected 2 attempts, got %d", attempts)
		}
	})

	t.Run("Max retries exceeded", func(t *testing.T) {
		attempts := 0
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				attempts++
				return &http.Response{
					StatusCode: 500,
					Body:       io.NopCloser(strings.NewReader(`{"error":"server error"}`)),
				}, nil
			},
		}

		client := NewClient(mock, "http://example.com")
		err := client.Call(context.Background(), "GET", "/test", nil, nil, nil, nil)

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if attempts != 4 {
			t.Errorf("expected 4 attempts, got %d", attempts)
		}
	})
}

func TestClient_Call_CircuitBreakerIgnoresClientErrors(t *testing.T) {
	t.Run("4xx does not trip breaker", func(t *testing.T) {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 400,
					Body:       io.NopCloser(strings.NewReader(`{"message":"bad request"}`)),
				}, nil
			},
		}

		cb := NewCircuitBreaker(CircuitBreakerConfig{
			MaxFailures:     1,
			ResetTimeout:    time.Second,
			HalfOpenMaxReqs: 1,
		})
		client := NewClient(mock, "http://example.com")
		client.SetCircuitBreaker(cb)

		err := client.Call(context.Background(), "GET", "/bad", nil, nil, nil, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var apiErr *types.Error
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected *types.Error, got %T", err)
		}
		if cb.Failures() != 0 {
			t.Errorf("Failures() = %d, want 0", cb.Failures())
		}
		if cb.State() != StateClosed {
			t.Errorf("State() = %v, want %v", cb.State(), StateClosed)
		}
	})

	t.Run("context cancellation does not trip breaker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, ctx.Err()
			},
		}

		cb := NewCircuitBreaker(CircuitBreakerConfig{
			MaxFailures:     1,
			ResetTimeout:    time.Second,
			HalfOpenMaxReqs: 1,
		})
		client := NewClient(mock, "http://example.com")
		client.SetCircuitBreaker(cb)

		err := client.Call(ctx, "GET", "/canceled", nil, nil, nil, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if cb.Failures() != 0 {
			t.Errorf("Failures() = %d, want 0", cb.Failures())
		}
		if cb.State() != StateClosed {
			t.Errorf("State() = %v, want %v", cb.State(), StateClosed)
		}
	})
}

func TestClientHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("Get", func(t *testing.T) {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{"id":"1"}`)),
				}, nil
			},
		}
		client := NewClient(mock, "http://example.com")
		var resp map[string]string
		err := client.Get(ctx, "/get", nil, &resp)
		if err != nil || resp["id"] != "1" {
			t.Errorf("Get failed: %v", err)
		}
	})

	t.Run("Post", func(t *testing.T) {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
				}, nil
			},
		}
		client := NewClient(mock, "http://example.com")
		err := client.Post(ctx, "/post", nil, nil)
		if err != nil {
			t.Errorf("Post failed: %v", err)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		client := NewClient(http.DefaultClient, "http://example.com")
		clone := client.CloneWithBaseURL("http://new.com")
		if clone.baseURL != "http://new.com" {
			t.Errorf("clone failed")
		}
	})

	t.Run("Clone preserves resilience settings", func(t *testing.T) {
		// Create client with rate limiter and circuit breaker
		client := NewClientWithResilience(http.DefaultClient, "http://example.com", 10, DefaultCircuitBreakerConfig())
		client.SetUserAgent("test-agent")
		client.SetUseServerTime(true)

		// Clone with new base URL
		clone := client.CloneWithBaseURL("http://new.com")

		// Verify base URL changed
		if clone.baseURL != "http://new.com" {
			t.Errorf("expected baseURL http://new.com, got %s", clone.baseURL)
		}

		// Verify resilience settings preserved
		if clone.rateLimiter == nil {
			t.Error("rate limiter not preserved in clone")
		}
		if clone.circuitBreaker == nil {
			t.Error("circuit breaker not preserved in clone")
		}
		if clone.rateLimiter != client.rateLimiter {
			t.Error("rate limiter should be shared between original and clone")
		}
		if clone.circuitBreaker != client.circuitBreaker {
			t.Error("circuit breaker should be shared between original and clone")
		}

		// Verify other settings preserved
		if clone.userAgent != client.userAgent {
			t.Errorf("userAgent not preserved: expected %s, got %s", client.userAgent, clone.userAgent)
		}
		if clone.useServerTime != client.useServerTime {
			t.Error("useServerTime not preserved")
		}
	})

	t.Run("Setters", func(t *testing.T) {
		client := NewClient(http.DefaultClient, "http://example.com")
		client.SetUserAgent("ua")
		client.SetUseServerTime(true)
		client.SetAuth(nil, nil)
		client.SetBuilderConfig(nil)
	})

	t.Run("CallWithHeaders", func(t *testing.T) {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("X-Test") != "val" {
					return nil, fmt.Errorf("missing header")
				}
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			},
		}
		client := NewClient(mock, "http://example.com")
		err := client.CallWithHeaders(ctx, "GET", "/", nil, nil, nil, map[string]string{"X-Test": "val"})
		if err != nil {
			t.Errorf("CallWithHeaders failed: %v", err)
		}
	})

	t.Run("ServerTime", func(t *testing.T) {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`12345`)),
				}, nil
			},
		}
		client := NewClient(mock, "http://example.com")
		ts, err := client.serverTime(ctx)
		if err != nil || ts != 12345 {
			t.Errorf("serverTime failed: %v", err)
		}
	})
}

func TestMarshalBody(t *testing.T) {
	cases := []struct {
		input    interface{}
		expected string
	}{
		{map[string]string{"a": "b"}, `{"a":"b"}`},
		{"string", "string"},
		{[]byte("bytes"), "bytes"},
		{nil, ""},
	}

	for _, c := range cases {
		mock := &MockDoer{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				var body []byte
				if req.Body != nil {
					body, _ = io.ReadAll(req.Body)
				}
				if string(body) != c.expected {
					t.Errorf("expected %q, got %q", c.expected, string(body))
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			},
		}
		client := NewClient(mock, "http://example.com")
		_ = client.Post(context.Background(), "/", c.input, nil)
	}
}
