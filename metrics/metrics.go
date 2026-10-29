// Package metrics 是一个极简的指标库: 计数器、仪表、直方图 (均支持标签)，
// 以 Prometheus 文本格式 (version 0.0.4) 暴露，可直接被 Prometheus 抓取。
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry 保存所有指标。
type Registry struct {
	mu   sync.Mutex
	cols map[string]collector
}

type collector interface {
	name() string
	write(w io.Writer)
}

func NewRegistry() *Registry { return &Registry{cols: map[string]collector{}} }

// Default 是进程级的默认注册表，引擎内部的各个模块都往这里登记指标。
var Default = NewRegistry()

func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.cols[c.name()]; ok {
		if _, isFunc := old.(*gaugeFunc); !isFunc {
			panic("metrics: duplicate registration of " + c.name())
		}
	}
	r.cols[c.name()] = c // 仪表函数允许被覆盖 (多个引擎实例时以最近注册的为准)
}

// Write 以 Prometheus 文本格式输出全部指标 (按名字排序)。
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.cols))
	for n := range r.cols {
		names = append(names, n)
	}
	cols := make([]collector, 0, len(names))
	sort.Strings(names)
	for _, n := range names {
		cols = append(cols, r.cols[n])
	}
	r.mu.Unlock()
	for _, c := range cols {
		c.write(w)
	}
}

// Handler 返回暴露 /metrics 和 /healthz 的 HTTP Handler。
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) { io.WriteString(w, "ok\n") })
	return mux
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

func labelString(names, values []string, extra ...string) string {
	var parts []string
	for i, n := range names {
		parts = append(parts, n+`="`+escapeLabel(values[i])+`"`)
	}
	parts = append(parts, extra...)
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func header(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, strings.ReplaceAll(help, "\n", " "), name, typ)
}

// ---- Counter ----

type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

type CounterVec struct {
	nm, help string
	labels   []string
	mu       sync.Mutex
	children map[string]*Counter
	values   map[string][]string
}

func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{nm: name, help: help, labels: labels, children: map[string]*Counter{}, values: map[string][]string{}}
	r.register(v)
	return v
}

// Counter 注册一个没有标签的计数器。
func (r *Registry) Counter(name, help string) *Counter { return r.CounterVec(name, help).With() }

func (v *CounterVec) name() string { return v.nm }

func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values", v.nm, len(v.labels)))
	}
	key := strings.Join(values, "\xff")
	v.mu.Lock()
	defer v.mu.Unlock()
	c := v.children[key]
	if c == nil {
		c = &Counter{}
		v.children[key] = c
		v.values[key] = append([]string(nil), values...)
	}
	return c
}

func (v *CounterVec) write(w io.Writer) {
	header(w, v.nm, v.help, "counter")
	v.mu.Lock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %d\n", v.nm, labelString(v.labels, v.values[k]), v.children[k].Value())
	}
	v.mu.Unlock()
}

// ---- Gauge ----

type gaugeFunc struct {
	nm, help string
	f        func() float64
}

// GaugeFunc 注册一个抓取时才求值的仪表。同名重复注册会覆盖之前的函数。
func (r *Registry) GaugeFunc(name, help string, f func() float64) {
	r.register(&gaugeFunc{nm: name, help: help, f: f})
}

func (g *gaugeFunc) name() string { return g.nm }
func (g *gaugeFunc) write(w io.Writer) {
	header(w, g.nm, g.help, "gauge")
	fmt.Fprintf(w, "%s %s\n", g.nm, formatFloat(g.f()))
}

// ---- Histogram ----

// DefBuckets 适合语句/锁等待耗时 (秒)。
var DefBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type Histogram struct {
	bounds  []float64
	mu      sync.Mutex
	counts  []uint64 // 每个桶 (非累计)，最后一个是 +Inf
	sum     float64
	samples uint64
}

func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // 第一个 >= v 的桶 (le 语义)
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.samples++
	h.mu.Unlock()
}

// Count 返回观测次数。
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.samples
}

type HistogramVec struct {
	nm, help string
	labels   []string
	bounds   []float64
	mu       sync.Mutex
	children map[string]*Histogram
	values   map[string][]string
}

func (r *Registry) HistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	v := &HistogramVec{nm: name, help: help, labels: labels, bounds: b, children: map[string]*Histogram{}, values: map[string][]string{}}
	r.register(v)
	return v
}

func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	return r.HistogramVec(name, help, buckets).With()
}

func (v *HistogramVec) name() string { return v.nm }

func (v *HistogramVec) With(values ...string) *Histogram {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values", v.nm, len(v.labels)))
	}
	key := strings.Join(values, "\xff")
	v.mu.Lock()
	defer v.mu.Unlock()
	h := v.children[key]
	if h == nil {
		h = &Histogram{bounds: v.bounds, counts: make([]uint64, len(v.bounds)+1)}
		v.children[key] = h
		v.values[key] = append([]string(nil), values...)
	}
	return h
}

func (v *HistogramVec) write(w io.Writer) {
	header(w, v.nm, v.help, "histogram")
	v.mu.Lock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := v.children[k]
		h.mu.Lock()
		var cum uint64
		for i, b := range v.bounds {
			cum += h.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", v.nm, labelString(v.labels, v.values[k], `le="`+formatFloat(b)+`"`), cum)
		}
		cum += h.counts[len(v.bounds)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", v.nm, labelString(v.labels, v.values[k], `le="+Inf"`), cum)
		fmt.Fprintf(w, "%s_sum%s %s\n", v.nm, labelString(v.labels, v.values[k]), formatFloat(h.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", v.nm, labelString(v.labels, v.values[k]), h.samples)
		h.mu.Unlock()
	}
	v.mu.Unlock()
}
