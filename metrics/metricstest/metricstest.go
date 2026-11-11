// Package metricstest provides a recording metrics.Handler for tests and benchmarks.
package metricstest

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/skme902/tq/metrics"
)

// Recorder is a metrics.Handler that keeps every value in memory, keyed by name and tag set.
// It is safe for concurrent use.
type Recorder struct {
	mu       sync.Mutex
	counters map[string]int64
	gauges   map[string]float64
	timers   map[string][]time.Duration
	tagKeys  map[string]struct{}
	tagVals  map[string]struct{}
}

var _ metrics.Handler = (*Recorder)(nil)

// New returns an empty Recorder.
func New() *Recorder {
	return &Recorder{
		counters: map[string]int64{},
		gauges:   map[string]float64{},
		timers:   map[string][]time.Duration{},
		tagKeys:  map[string]struct{}{},
		tagVals:  map[string]struct{}{},
	}
}

// Key formats name and tags as name{k1=v1,k2=v2} with tags sorted by key, which is how values
// are looked up.
func Key(name string, tags ...metrics.Tag) string {
	ts := slices.Clone(tags)
	sort.SliceStable(ts, func(i, j int) bool { return ts[i].Key < ts[j].Key })
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, t := range ts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(t.Key)
		b.WriteByte('=')
		b.WriteString(t.Value)
	}
	b.WriteByte('}')
	return b.String()
}

func (r *Recorder) note(tags []metrics.Tag) {
	for _, t := range tags {
		r.tagKeys[t.Key] = struct{}{}
		r.tagVals[t.Value] = struct{}{}
	}
}

func (r *Recorder) Counter(name string, delta int64, tags ...metrics.Tag) {
	k := Key(name, tags...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tags)
	r.counters[k] += delta
}

func (r *Recorder) Gauge(name string, value float64, tags ...metrics.Tag) {
	k := Key(name, tags...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tags)
	r.gauges[k] = value
}

func (r *Recorder) Timer(name string, d time.Duration, tags ...metrics.Tag) {
	k := Key(name, tags...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tags)
	r.timers[k] = append(r.timers[k], d)
}

// CounterValue returns the sum of a counter series (0 if never recorded).
func (r *Recorder) CounterValue(name string, tags ...metrics.Tag) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[Key(name, tags...)]
}

// GaugeValue returns the last value of a gauge series and whether it was recorded.
func (r *Recorder) GaugeValue(name string, tags ...metrics.Tag) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.gauges[Key(name, tags...)]
	return v, ok
}

// TimerValues returns every recorded duration of a timer series.
func (r *Recorder) TimerValues(name string, tags ...metrics.Tag) []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.timers[Key(name, tags...)])
}

// Series returns every recorded series key (see Key) whose name is name, sorted.
func (r *Recorder) Series(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	prefix := name + "{"
	for _, m := range []map[string]struct{}{keysOf(r.counters), keysOf(r.gauges), keysOf(r.timers)} {
		for k := range m {
			if strings.HasPrefix(k, prefix) {
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

// HasTagValue reports whether any metric was recorded with a tag of this value.
func (r *Recorder) HasTagValue(v string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.tagVals[v]
	return ok
}

// TagKeys returns every tag key used, sorted.
func (r *Recorder) TagKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.tagKeys))
	for k := range r.tagKeys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func keysOf[V any](m map[string]V) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}
