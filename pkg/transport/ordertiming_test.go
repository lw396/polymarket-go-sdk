package transport

import (
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestOrderAuthTimeSourcesAndUnavailable(t *testing.T) {
	for _, source := range []string{"local", "server_time", "calibrated", "clock_unavailable", "server_time_failed", "PRIVATE_SOURCE", ""} {
		t.Run(source, func(t *testing.T) {
			ctx, c := WithOrderDiagnostics(t.Context())
			recordOrderAuthTime(ctx, source, nil)
			got := c.Finish().AuthTime[0]
			want := source
			if source == "PRIVATE_SOURCE" || source == "" {
				want = "unknown"
			}
			if got != (OrderAuthTime{Source: want, Age: -1, Uncertainty: -1, MaxPastSkew: -1, MaxFutureSkew: -1}) {
				t.Fatalf("unsafe source or missing evidence became zero: %+v", got)
			}
		})
	}
	recordOrderAuthTime(nil, "local", nil)
	recordOrderAuthTime(t.Context(), "local", nil)
}

func TestOrderAuthTimeBoundFreezeAndCopy(t *testing.T) {
	ctx, c := WithOrderDiagnostics(t.Context())
	reading := &AuthClockReading{Timestamp: 123456, RTT: 99, Age: 0, Uncertainty: 2, MaxPastSkew: 3, MaxFutureSkew: 4}
	recordOrderAuthTime(ctx, "calibrated", reading)
	reading.Age = 123
	var wg sync.WaitGroup
	for range 99 {
		wg.Go(func() { recordOrderAuthTime(ctx, "local", nil) })
	}
	wg.Wait()
	got := c.Finish()
	want := OrderAuthTime{Source: "calibrated", Age: 0, Uncertainty: 2, MaxPastSkew: 3, MaxFutureSkew: 4}
	if len(got.AuthTime) != 32 || got.DroppedAuthTime != 68 || got.AuthTime[0] != want {
		t.Fatalf("unbounded, aliased or lost evidence: %+v", got)
	}
	got.AuthTime[0].Source = "mutated"
	recordOrderAuthTime(ctx, "server_time_failed", nil)
	frozen := c.Finish()
	if len(frozen.AuthTime) != 32 || frozen.DroppedAuthTime != 68 || frozen.AuthTime[0] != want {
		t.Fatalf("finish did not freeze or clone: %+v", frozen)
	}
	_, empty := WithOrderDiagnostics(t.Context())
	if got := empty.Finish(); len(got.AuthTime) != 0 || got.DroppedAuthTime != 0 {
		t.Fatalf("empty collector has evidence: %+v", got)
	}
}

func TestOrderDiagnosticsTraceIsBoundedAndFrozen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, collector := WithOrderDiagnostics(t.Context())
		ctx = WithOrderPhase(ctx, "build")
		calls := 0
		var late *httptrace.ClientTrace
		client := NewClient(&MockDoer{DoFunc: func(req *http.Request) (*http.Response, error) {
			calls++
			trace := httptrace.ContextClientTrace(req.Context())
			late = trace
			trace.GetConn("PRIVATE_HOST")
			trace.DNSStart(httptrace.DNSStartInfo{Host: "PRIVATE_HOST"})
			time.Sleep(time.Millisecond)
			trace.DNSDone(httptrace.DNSDoneInfo{})
			trace.ConnectStart("tcp", "PRIVATE_ADDRESS")
			time.Sleep(2 * time.Millisecond)
			trace.ConnectDone("tcp", "PRIVATE_ADDRESS", nil)
			trace.TLSHandshakeStart()
			time.Sleep(3 * time.Millisecond)
			trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
			trace.GotConn(httptrace.GotConnInfo{Reused: false})
			trace.WroteRequest(httptrace.WroteRequestInfo{})
			time.Sleep(4 * time.Millisecond)
			trace.GotFirstResponseByte()
			status := 200
			if calls == 1 {
				status = 500
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
		}}, "http://synthetic.invalid")
		if err := client.Get(ctx, "/tick-size", nil, nil); err != nil {
			t.Fatal(err)
		}
		RecordTickCache(ctx, true)
		RecordTickCache(ctx, false)
		RecordOrderSigning(ctx, 5*time.Millisecond)
		got := collector.Finish()
		if len(got.HTTP) != 2 || got.HTTP[0].Attempt != 1 || got.HTTP[1].Attempt != 2 || got.HTTP[0].Status != 500 || got.TickCacheHits != 1 || got.TickCacheMisses != 1 || got.Signing != 5*time.Millisecond || got.SigningCalls != 1 {
			t.Fatalf("wrong observations: %+v", got)
		}
		for _, r := range got.HTTP {
			if r.Endpoint != "tick_size" || r.Phase != "build" || r.DNS != time.Millisecond || r.Connect != 2*time.Millisecond || r.TLS != 3*time.Millisecond || r.ConnectionWait != 6*time.Millisecond || r.WriteToFirstByte != 4*time.Millisecond || r.Total != 10*time.Millisecond || r.Writes != 1 || !r.ConnectionObserved || r.Reused || !r.BodyComplete {
				t.Fatalf("wrong trace: %+v", r)
			}
		}
		late.WroteRequest(httptrace.WroteRequestInfo{})
		RecordTickCache(ctx, true)
		got.HTTP[0].Endpoint = "mutated"
		frozen := collector.Finish()
		if frozen.HTTP[0].Endpoint != "tick_size" || frozen.TickCacheHits != 1 || frozen.HTTP[1].Writes != 1 {
			t.Fatal("late callback or snapshot alias changed result")
		}
		ctx2, c2 := WithOrderDiagnostics(t.Context())
		empty := c2.Finish()
		if empty.Signing != -1 || len(empty.HTTP) != 0 {
			t.Fatal("unobserved signature became zero")
		}
		RecordOrderSigning(ctx2, 0)
	})
}

func TestOrderDiagnosticsFailureAndServerTime(t *testing.T) {
	for _, tt := range []struct {
		name string
		fail bool
	}{{"server time", false}, {"failed request", true}} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, c := WithOrderDiagnostics(t.Context())
				client := NewClient(&MockDoer{DoFunc: func(req *http.Request) (*http.Response, error) {
					if tt.fail {
						return nil, errors.New("PRIVATE_ERROR")
					}
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`123`)), Request: req}, nil
				}}, "http://synthetic.invalid")
				_, err := client.serverTime(ctx)
				got := c.Finish()
				if (err != nil) != tt.fail || len(got.HTTP) != 1 || got.HTTP[0].Endpoint != "time" || got.HTTP[0].DNS != -1 || got.HTTP[0].WriteToFirstByte != -1 || got.HTTP[0].BodyComplete == tt.fail {
					t.Fatalf("wrong availability: %+v", got)
				}
			})
		})
	}
}

func TestOrderDiagnosticsBoundsPendingConnects(t *testing.T) {
	ctx, c := WithOrderDiagnostics(t.Context())
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://synthetic.invalid/tick-size", nil)
	req, finish := observeOrderHTTP(req, "/tick-size", 1)
	trace := httptrace.ContextClientTrace(req.Context())
	for i := range 100 {
		trace.ConnectStart("tcp", strings.Repeat("x", i+1))
	}
	for i := range 100 {
		trace.ConnectDone("tcp", strings.Repeat("x", i+1), nil)
	}
	finish(200, true, 0)
	got := c.Finish()
	if len(got.HTTP) != 1 || !got.HTTP[0].ConnectSamplesTruncated || got.HTTP[0].Connect != -1 {
		t.Fatalf("incomplete connect timings claimed complete: %+v", got)
	}
	trace.ConnectStart("tcp", "late")
	trace.ConnectDone("tcp", "late", nil)
	if !c.Finish().HTTP[0].ConnectSamplesTruncated {
		t.Fatal("late callback changed frozen result")
	}
}

func TestOrderDiagnosticsRejectsAmbiguousSameAddressConnects(t *testing.T) {
	ctx, c := WithOrderDiagnostics(t.Context())
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://synthetic.invalid/tick-size", nil)
	req, finish := observeOrderHTTP(req, "/tick-size", 1)
	trace := httptrace.ContextClientTrace(req.Context())
	trace.ConnectStart("tcp", "same")
	trace.ConnectStart("tcp", "same")
	trace.ConnectDone("tcp", "same", nil)
	trace.ConnectDone("tcp", "same", nil)
	finish(200, true, 0)
	got := c.Finish().HTTP[0]
	if got.Connect != -1 || !got.ConnectSamplesTruncated {
		t.Fatalf("ambiguous dial measured as complete: %+v", got)
	}
}

func TestOrderDiagnosticsAmbiguousSetupRemainsUnknown(t *testing.T) {
	ctx, c := WithOrderDiagnostics(t.Context())
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://synthetic.invalid/tick-size", nil)
	req, finish := observeOrderHTTP(req, "/tick-size", 1)
	trace := httptrace.ContextClientTrace(req.Context())
	trace.DNSStart(httptrace.DNSStartInfo{})
	trace.DNSStart(httptrace.DNSStartInfo{})
	trace.DNSDone(httptrace.DNSDoneInfo{})
	trace.DNSDone(httptrace.DNSDoneInfo{})
	trace.TLSHandshakeStart()
	trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
	trace.TLSHandshakeStart() // An incomplete second handshake must not retain the first duration.
	finish(0, false, -1)
	got := c.Finish().HTTP[0]
	if got.DNS != -1 || got.TLS != -1 {
		t.Fatalf("ambiguous/incomplete setup claimed measured: %+v", got)
	}
}

type incompleteDiagnosticBody struct{}

func (incompleteDiagnosticBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (incompleteDiagnosticBody) Close() error             { return nil }

func TestOrderDiagnosticsTruncatedBodyAndNoCollector(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, c := WithOrderDiagnostics(t.Context())
		client := NewClient(&MockDoer{DoFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: incompleteDiagnosticBody{}, Request: req}, nil
		}}, "http://synthetic.invalid")
		if err := client.Get(ctx, "/tick-size", nil, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("error replaced: %v", err)
		}
		got := c.Finish()
		if len(got.HTTP) != 4 {
			t.Fatalf("retry count changed: %+v", got)
		}
		for i, r := range got.HTTP {
			if r.BodyComplete || r.Status != 200 || r.Attempt != i+1 || r.BodyRead < 0 {
				t.Fatalf("body availability incorrect: %+v", r)
			}
		}
		req, _ := http.NewRequestWithContext(t.Context(), "GET", "http://synthetic.invalid/tick-size", nil)
		observed, finish := observeOrderHTTP(req, "/tick-size", 1)
		finish(200, true, 0)
		if req != observed || httptrace.ContextClientTrace(observed.Context()) != nil {
			t.Fatal("opt-out acquired tracing")
		}
	})
}

func TestOrderDiagnosticsConcurrentBound(t *testing.T) {
	ctx, c := WithOrderDiagnostics(t.Context())
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			client := NewClient(&MockDoer{DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
			}}, "http://synthetic.invalid")
			_ = client.Get(ctx, "/tick-size", nil, nil)
		})
	}
	wg.Wait()
	got := c.Finish()
	if len(got.HTTP) != 32 || got.DroppedHTTP != 68 {
		t.Fatalf("unbounded or silent loss: %d/%d", len(got.HTTP), got.DroppedHTTP)
	}
}
