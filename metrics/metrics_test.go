package metrics

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("req_total", "Requests.", "method", "code")
	c.With("GET", "200").Add(3)
	c.With("POST", `5"00`).Inc()
	plain := r.Counter("plain_total", "No labels.")
	plain.Inc()
	r.GaugeFunc("temp", "A gauge.", func() float64 { return 21.5 })
	h := r.HistogramVec("lat_seconds", "Latency.", []float64{0.1, 1}, "op")
	h.With("read").Observe(0.05)
	h.With("read").Observe(0.5)
	h.With("read").Observe(7)
	var buf bytes.Buffer
	r.Write(&buf)
	out := buf.String()
	for _, want := range []string{
		"# TYPE req_total counter",
		`req_total{method="GET",code="200"} 3`,
		`req_total{method="POST",code="5\"00"} 1`,
		"plain_total 1",
		"# TYPE temp gauge",
		"temp 21.5",
		"# TYPE lat_seconds histogram",
		`lat_seconds_bucket{op="read",le="0.1"} 1`,
		`lat_seconds_bucket{op="read",le="1"} 2`,
		`lat_seconds_bucket{op="read",le="+Inf"} 3`,
		`lat_seconds_sum{op="read"} 7.55`,
		`lat_seconds_count{op="read"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// 仪表函数允许覆盖；计数器重复注册会 panic
	r.GaugeFunc("temp", "A gauge.", func() float64 { return 30 })
	buf.Reset()
	r.Write(&buf)
	if !strings.Contains(buf.String(), "temp 30") {
		t.Fatal("gauge func not replaced")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("duplicate counter registration must panic")
			}
		}()
		r.Counter("plain_total", "dup")
	}()
}

func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.Counter("x_total", "x").Add(2)
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	for path, want := range map[string]string{"/metrics": "x_total 2", "/healthz": "ok"} {
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		b.ReadFrom(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(b.String(), want) {
			t.Fatalf("%s: %d %q", path, resp.StatusCode, b.String())
		}
		if path == "/metrics" && !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
			t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("c_total", "c", "k")
	h := r.Histogram("h_seconds", "h", DefBuckets)
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 1000; i++ {
				c.With(string(rune('a' + g%3))).Inc()
				h.Observe(float64(i) / 1000)
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	total := c.With("a").Value() + c.With("b").Value() + c.With("c").Value()
	if total != 8000 || h.Count() != 8000 {
		t.Fatalf("lost updates: %d %d", total, h.Count())
	}
}
