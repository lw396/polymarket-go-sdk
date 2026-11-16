package transport

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"time"
)

// OrderDiagnostics is a bounded, payload-free snapshot. Durations are
// nanoseconds; -1 means unobserved, not zero. HTTP intervals can overlap.
// These observations never establish order rejection or matching time.
type OrderDiagnostics struct {
	TickCacheHits   int
	TickCacheMisses int
	Signing         time.Duration
	SigningCalls    int
	HTTP            []OrderHTTPTime
	DroppedHTTP     int
}

// OrderHTTPTime describes one SDK HTTP Do attempt, including its redirects and
// any hidden transport replay. Writes is only the callbacks observed by finish,
// not a proof of submission count. No URL, address, header, body or error is held.
type OrderHTTPTime struct {
	Endpoint                string
	Phase                   string
	Attempt                 int
	Status                  int
	Total                   time.Duration
	DNS                     time.Duration
	Connect                 time.Duration
	TLS                     time.Duration
	ConnectionWait          time.Duration
	WriteToFirstByte        time.Duration
	BodyRead                time.Duration
	ConnectionObserved      bool
	Reused                  bool
	Writes                  int
	BodyComplete            bool
	ConnectSamplesTruncated bool
}

type orderDiagnosticKey struct{}
type orderPhaseKey struct{}

// OrderDiagnosticCollector belongs to one order context. Finish seals it and
// returns a defensive copy, so late trace callbacks cannot mutate the snapshot.
type OrderDiagnosticCollector struct {
	mu     sync.Mutex
	closed bool
	data   OrderDiagnostics
}

func WithOrderDiagnostics(ctx context.Context) (context.Context, *OrderDiagnosticCollector) {
	c := &OrderDiagnosticCollector{data: OrderDiagnostics{Signing: -1}}
	return context.WithValue(ctx, orderDiagnosticKey{}, c), c
}

// WithOrderPhase accepts only fixed stage labels, never caller-provided text.
func WithOrderPhase(ctx context.Context, phase string) context.Context {
	if phase != "build" && phase != "submit" {
		phase = "unknown"
	}
	return context.WithValue(ctx, orderPhaseKey{}, phase)
}

func diagnosticCollector(ctx context.Context) *OrderDiagnosticCollector {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(orderDiagnosticKey{}).(*OrderDiagnosticCollector)
	return c
}

func (c *OrderDiagnosticCollector) Finish() OrderDiagnostics {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	result := c.data
	result.HTTP = slices.Clone(result.HTTP)
	return result
}

func RecordTickCache(ctx context.Context, hit bool) {
	if c := diagnosticCollector(ctx); c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.closed {
			if hit {
				c.data.TickCacheHits++
			} else {
				c.data.TickCacheMisses++
			}
		}
	}
}

// RecordOrderSigning measures local signing only, not HMAC/time lookup or HTTP.
func RecordOrderSigning(ctx context.Context, d time.Duration) {
	if c := diagnosticCollector(ctx); c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.closed {
			if c.data.SigningCalls == 0 {
				c.data.Signing = 0
			}
			c.data.Signing += d
			c.data.SigningCalls++
		}
	}
}

func observeOrderHTTP(req *http.Request, path string, attempt int) (*http.Request, func(int, bool, time.Duration)) {
	collector := diagnosticCollector(req.Context())
	endpoint := ""
	switch {
	case path == "/time":
		endpoint = "time"
	case path == "/tick-size" || strings.HasPrefix(path, "/tick-size/"):
		endpoint = "tick_size"
	case path == "/order" && req.Method == http.MethodPost:
		endpoint = "order"
	}
	if collector == nil || endpoint == "" {
		return req, func(int, bool, time.Duration) {}
	}
	phase, _ := req.Context().Value(orderPhaseKey{}).(string)
	if phase == "" {
		phase = "unknown"
	}
	r := OrderHTTPTime{Endpoint: endpoint, Phase: phase, Attempt: attempt, DNS: -1, Connect: -1, TLS: -1, ConnectionWait: -1, WriteToFirstByte: -1, BodyRead: -1}
	var mu sync.Mutex
	var frozen bool
	started := time.Now()
	var dnsAt, tlsAt, connAt, writtenAt, firstByteAt time.Time
	var dnsAmbiguous, tlsAmbiguous bool
	connects := make(map[string]time.Time)
	add := func(total *time.Duration, d time.Duration) {
		if *total < 0 {
			*total = 0
		}
		*total += d
	}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen {
				connAt = time.Now()
			}
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen {
				if !dnsAt.IsZero() {
					dnsAmbiguous = true
					r.DNS = -1
				}
				dnsAt = time.Now()
			}
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen && !dnsAmbiguous && !dnsAt.IsZero() {
				add(&r.DNS, time.Since(dnsAt))
				dnsAt = time.Time{}
			}
		},
		ConnectStart: func(network, address string) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen && !r.ConnectSamplesTruncated {
				key := network + "\x00" + address
				_, duplicate := connects[key]
				if duplicate || len(connects) >= 32 || len(key) > 512 {
					r.ConnectSamplesTruncated = true
					r.Connect = -1
					clear(connects)
				} else {
					connects[key] = time.Now()
				}
			}
		},
		ConnectDone: func(network, address string, _ error) {
			mu.Lock()
			defer mu.Unlock()
			key := network + "\x00" + address
			if at, ok := connects[key]; !frozen && !r.ConnectSamplesTruncated && ok {
				add(&r.Connect, time.Since(at))
				delete(connects, key)
			}
		},
		TLSHandshakeStart: func() {
			mu.Lock()
			defer mu.Unlock()
			if !frozen {
				if !tlsAt.IsZero() {
					tlsAmbiguous = true
					r.TLS = -1
				}
				tlsAt = time.Now()
			}
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen && !tlsAmbiguous && !tlsAt.IsZero() {
				add(&r.TLS, time.Since(tlsAt))
				tlsAt = time.Time{}
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen {
				r.ConnectionObserved = true
				r.Reused = info.Reused
				if !connAt.IsZero() {
					add(&r.ConnectionWait, time.Since(connAt))
					connAt = time.Time{}
				}
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			mu.Lock()
			defer mu.Unlock()
			if !frozen {
				r.Writes++
				if info.Err == nil && writtenAt.IsZero() {
					writtenAt = time.Now()
				}
			}
		},
		GotFirstResponseByte: func() {
			mu.Lock()
			defer mu.Unlock()
			if !frozen && firstByteAt.IsZero() {
				firstByteAt = time.Now()
				if !writtenAt.IsZero() {
					r.WriteToFirstByte = firstByteAt.Sub(writtenAt)
				}
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	finish := func(status int, complete bool, bodyRead time.Duration) {
		mu.Lock()
		if frozen {
			mu.Unlock()
			return
		}
		frozen = true
		if len(connects) > 0 {
			r.ConnectSamplesTruncated = true
			r.Connect = -1
		}
		clear(connects)
		if !dnsAt.IsZero() {
			r.DNS = -1
		}
		if !tlsAt.IsZero() {
			r.TLS = -1
		}
		if !connAt.IsZero() {
			r.ConnectionWait = -1
		}
		r.Status = status
		r.BodyComplete = complete
		r.Total = time.Since(started)
		r.BodyRead = bodyRead
		result := r
		mu.Unlock()
		collector.mu.Lock()
		defer collector.mu.Unlock()
		if collector.closed {
			return
		}
		if len(collector.data.HTTP) < 32 {
			collector.data.HTTP = append(collector.data.HTTP, result)
		} else {
			collector.data.DroppedHTTP++
		}
	}
	return req, finish
}
