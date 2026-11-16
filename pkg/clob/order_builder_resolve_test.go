package clob

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/clob/clobtypes"
	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/transport"
)

func requireTickLookupForTest(t *testing.T, b *OrderBuilder) *OrderBuilder {
	t.Helper()
	return b.RequireTickSizeLookup()
}

func TestBuilderTickLookupInvalidationWithOverride(t *testing.T) {
	for _, market := range []bool{false, true} {
		name := "limit"
		if market {
			name = "market"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			doer := metadataDoer(func(req *http.Request) (*http.Response, error) {
				close(started)
				<-release
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"minimum_tick_size":0.01}`)), Header: make(http.Header), Request: req}, nil
			})
			client := NewClient(transport.NewClient(doer, "http://synthetic.invalid"))
			b := NewOrderBuilder(client, mustSigner(t)).TokenID("123").Side("BUY").Price(0.5).Size(2).AmountUSDC(1).TickSize(0.01)
			type result struct {
				order *clobtypes.SignableOrder
				err   error
			}
			done := make(chan result, 1)
			go func() {
				var order *clobtypes.SignableOrder
				var err error
				if market {
					order, err = b.BuildMarketWithContext(t.Context())
				} else {
					order, err = b.BuildSignableWithContext(t.Context())
				}
				done <- result{order, err}
			}()
			<-started
			client.InvalidateCaches()
			close(release)
			got := <-done
			if got.order != nil || !errors.Is(got.err, errTickSizeCacheChanged) {
				t.Fatalf("build after invalidation = %v, %v; want nil order and cache-changed error", got.order, got.err)
			}
		})
	}
}

func TestBuilderRequireTickLookupHTTPFailure(t *testing.T) {
	for _, required := range []bool{false, true} {
		name := "default-fallback"
		if required {
			name = "required-error"
		}
		t.Run(name, func(t *testing.T) {
			doer := metadataDoer(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(`{"error":"metadata unavailable"}`)), Header: make(http.Header), Request: req}, nil
			})
			client := NewClient(transport.NewClient(doer, "http://synthetic.invalid"))
			b := NewOrderBuilder(client, mustSigner(t)).TokenID("123").Side("BUY").Price(0.5).Size(2).TickSize(0.01)
			if required {
				b = requireTickLookupForTest(t, b)
			}
			order, err := b.BuildSignableWithContext(t.Context())
			if required {
				if order != nil || err == nil || !strings.Contains(err.Error(), "metadata unavailable") {
					t.Fatalf("required lookup = %v, %v; want original metadata failure and no order", order, err)
				}
			} else if order == nil || err != nil {
				t.Fatalf("default override fallback = %v, %v", order, err)
			}
		})
	}
}

func TestBuilderRequireTickLookupValidatesResponse(t *testing.T) {
	for _, body := range []string{`{}`, `{"minimum_tick_size":0}`, `{"minimum_tick_size":-0.01}`, `{"minimum_tick_size":1}`} {
		t.Run(body, func(t *testing.T) {
			client := NewClient(transport.NewClient(metadataDoer(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
			}), "http://synthetic.invalid"))
			b := NewOrderBuilder(client, mustSigner(t)).TokenID("123").Side("BUY").Price(0.5).Size(2).TickSize(0.01).RequireTickSizeLookup()
			if order, err := b.BuildSignableWithContext(t.Context()); err == nil || order != nil {
				t.Fatalf("invalid metadata allowed order: %v %v", order, err)
			}
		})
	}
}

func TestBuilderRequireTickLookupOfflineOverride(t *testing.T) {
	for _, client := range []Client{nil, &clientImpl{}} {
		b := NewOrderBuilder(client, mustSigner(t)).TokenID("123").Side("BUY").Price(0.5).Size(2).TickSize(0.01)
		b = requireTickLookupForTest(t, b)
		if order, err := b.BuildSignableWithContext(t.Context()); err != nil || order == nil {
			t.Fatalf("offline override = %v, %v", order, err)
		}
	}
}
