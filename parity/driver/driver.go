// Package driver replays a seeded parity workload script against an implementation and
// records a trace. It is stdlib-only on purpose: parity/run_upstream.sh copies this file (with the
// package clause rewritten) into an overlay of the upstream service/matching package, so
// upstream and our library are driven by byte-identical scheduling code.
package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Script is a generated workload.
type Script struct {
	Name      string   `json:"name"`
	Seed      int64    `json:"seed"`
	Namespace string   `json:"namespace"`
	Queues    []string `json:"queues"`
	// LongPollMS is the long-poll expiration used by both implementations.
	LongPollMS int `json:"long_poll_ms"`
	// Deterministic marks scripts where dispatch order should be reproducible
	// (all adds finish before a single poller starts).
	Deterministic bool    `json:"deterministic"`
	Events        []Event `json:"events"`
	// DrainTimeoutMS bounds the final "await_all" wait.
	DrainTimeoutMS int `json:"drain_timeout_ms"`
}

// Event ops: add, start (poller), stop (poller), queue_rps, key_rps, weights,
// barrier (wait for all prior adds, sleep SettleMS, rebase the clock), mark (record a
// named timestamp, name in Key), await_all (wait until every added task is delivered).
type Event struct {
	AtMS     int64              `json:"at_ms"`
	Op       string             `json:"op"`
	Queue    string             `json:"q,omitempty"`
	ID       int64              `json:"id,omitempty"`
	Pri      int32              `json:"pri,omitempty"`
	Key      string             `json:"key,omitempty"`
	Weight   float32            `json:"w,omitempty"`
	Poller   string             `json:"poller,omitempty"`
	RPS      *float32           `json:"rps,omitempty"`
	Weights  map[string]float32 `json:"weights,omitempty"`
	SettleMS int64              `json:"settle_ms,omitempty"`
}

// Impl is one implementation under test.
type Impl interface {
	// Add enqueues task id. It reports whether it was sync-matched.
	Add(ctx context.Context, queue string, id int64, pri int32, key string, weight float32) (bool, error)
	// Poll long-polls once. Implementations call Recorder.Delivered for every task handed
	// to a consumer (upstream: inside RecordActivityTaskStarted; ours: when PollTask returns).
	Poll(ctx context.Context, queue, poller string) error
	SetQueueRPS(ctx context.Context, queue string, rps *float32) error
	SetKeyRPS(ctx context.Context, queue string, rps *float32) error
	SetWeights(ctx context.Context, queue string, w map[string]float32) error
}

type AddRec struct {
	ID     int64  `json:"id"`
	Queue  string `json:"q"`
	AtNS   int64  `json:"at"`
	DoneNS int64  `json:"done"`
	Sync   bool   `json:"sync"`
	Err    string `json:"err,omitempty"`
	Tries  int    `json:"tries,omitempty"`
}

type DelRec struct {
	ID     int64  `json:"id"`
	Queue  string `json:"q"`
	Poller string `json:"poller"`
	AtNS   int64  `json:"at"`
}

type Trace struct {
	Impl       string   `json:"impl"`
	Store      string   `json:"store"`
	Partitions int      `json:"partitions"`
	Script     string   `json:"script"`
	Run        int      `json:"run"`
	Started    string   `json:"started"`
	Adds       []AddRec `json:"adds"`
	Deliveries []DelRec `json:"deliveries"`
	PollErrors int      `json:"poll_errors"`
	ErrSamples []string `json:"err_samples,omitempty"`
	ElapsedMS  int64    `json:"elapsed_ms"`
	// Marks are named timestamps (ns since start) recorded by "mark" events.
	Marks    map[string]int64 `json:"marks,omitempty"`
	Complete bool             `json:"complete"`
}

// Recorder collects deliveries; safe for concurrent use.
type Recorder struct {
	mu    sync.Mutex
	start time.Time
	tr    *Trace
	added map[int64]bool
	got   map[int64]bool
	allCh chan struct{}
	want  int
}

func (r *Recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

// Delivered records one task delivery.
func (r *Recorder) Delivered(queue string, id int64, poller string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tr.Deliveries = append(r.tr.Deliveries, DelRec{ID: id, Queue: queue, Poller: poller, AtNS: r.now()})
	r.got[id] = true
	r.checkAll()
}

func (r *Recorder) checkAll() {
	if r.allCh == nil || r.want == 0 {
		return
	}
	n := 0
	for id := range r.added {
		if r.got[id] {
			n++
		}
	}
	if n >= r.want {
		close(r.allCh)
		r.allCh = nil
	}
}

func (r *Recorder) pollErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tr.PollErrors++
	if len(r.tr.ErrSamples) < 10 {
		r.tr.ErrSamples = append(r.tr.ErrSamples, "poll: "+err.Error())
	}
}

// NewRecorder must be created before the Impl so the Impl can report deliveries.
func NewRecorder(impl, store string, partitions int, script string, run int) *Recorder {
	return &Recorder{
		tr:    &Trace{Impl: impl, Store: store, Partitions: partitions, Script: script, Run: run},
		added: map[int64]bool{}, got: map[int64]bool{},
	}
}

func LoadScript(path string) (*Script, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Script
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Run replays s against impl and returns the trace.
func Run(s *Script, impl Impl, rec *Recorder) *Trace {
	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	rec.start = time.Now()
	rec.tr.Started = rec.start.Format(time.RFC3339Nano)

	// One adder goroutine per queue keeps per-queue add order equal to script order.
	adders := map[string]chan Event{}
	var addWG sync.WaitGroup   // all adders' goroutines
	var pending sync.WaitGroup // adds not yet completed
	adder := func(q string) chan Event {
		if ch, ok := adders[q]; ok {
			return ch
		}
		ch := make(chan Event, 1<<16)
		adders[q] = ch
		addWG.Add(1)
		go func() {
			defer addWG.Done()
			for ev := range ch {
				at := rec.now()
				var sync bool
				var err error
				tries := 0
				// Retry transient add errors like a real client would (same policy both sides).
				for deadline := time.Now().Add(5 * time.Second); ; {
					tries++
					sync, err = impl.Add(ctx, ev.Queue, ev.ID, ev.Pri, ev.Key, ev.Weight)
					if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				r := AddRec{ID: ev.ID, Queue: ev.Queue, AtNS: at, DoneNS: rec.now(), Sync: sync, Tries: tries}
				rec.mu.Lock()
				if err != nil {
					r.Err = err.Error()
				} else {
					rec.added[ev.ID] = true
				}
				rec.tr.Adds = append(rec.tr.Adds, r)
				rec.mu.Unlock()
				pending.Done()
			}
		}()
		return ch
	}

	type poller struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	pollers := map[string]*poller{}
	startPoller := func(q, name string) {
		key := q + "/" + name
		if _, ok := pollers[key]; ok {
			return
		}
		pctx, cancel := context.WithCancel(ctx)
		p := &poller{cancel: cancel, done: make(chan struct{})}
		pollers[key] = p
		go func() {
			defer close(p.done)
			for pctx.Err() == nil {
				if err := impl.Poll(pctx, q, name); err != nil && pctx.Err() == nil {
					rec.pollErr(err)
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}
	stopPoller := func(q, name string) {
		key := q + "/" + name
		if p, ok := pollers[key]; ok {
			p.cancel()
			<-p.done
			delete(pollers, key)
		}
	}

	base := time.Now()
	for _, ev := range s.Events {
		if d := time.Until(base.Add(time.Duration(ev.AtMS) * time.Millisecond)); d > 0 {
			time.Sleep(d)
		}
		switch ev.Op {
		case "add":
			pending.Add(1)
			adder(ev.Queue) <- ev
		case "start":
			startPoller(ev.Queue, ev.Poller)
		case "stop":
			stopPoller(ev.Queue, ev.Poller)
		case "queue_rps", "key_rps", "weights":
			var err error
			switch ev.Op {
			case "queue_rps":
				err = impl.SetQueueRPS(ctx, ev.Queue, ev.RPS)
			case "key_rps":
				err = impl.SetKeyRPS(ctx, ev.Queue, ev.RPS)
			default:
				err = impl.SetWeights(ctx, ev.Queue, ev.Weights)
			}
			if err != nil {
				panic(fmt.Sprintf("config %s %s: %v", ev.Op, ev.Queue, err))
			}
		case "barrier":
			pending.Wait()
			time.Sleep(time.Duration(ev.SettleMS) * time.Millisecond)
			base = time.Now().Add(-time.Duration(ev.AtMS) * time.Millisecond)
		case "mark":
			rec.mu.Lock()
			if rec.tr.Marks == nil {
				rec.tr.Marks = map[string]int64{}
			}
			rec.tr.Marks[ev.Key] = rec.now()
			rec.mu.Unlock()
		case "await_all":
			pending.Wait()
			rec.mu.Lock()
			rec.want = len(rec.added)
			ch := make(chan struct{})
			rec.allCh = ch
			rec.checkAll()
			rec.mu.Unlock()
			select {
			case <-ch:
				rec.tr.Complete = true
			case <-time.After(time.Duration(s.DrainTimeoutMS) * time.Millisecond):
			}
		default:
			panic("unknown op " + ev.Op)
		}
	}
	pending.Wait()
	keys := make([]string, 0, len(pollers))
	for k := range pollers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		pollers[k].cancel()
	}
	for _, k := range keys {
		<-pollers[k].done
	}
	for _, ch := range adders {
		close(ch)
	}
	addWG.Wait()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.tr.ElapsedMS = time.Since(rec.start).Milliseconds()
	sort.SliceStable(rec.tr.Adds, func(i, j int) bool { return rec.tr.Adds[i].AtNS < rec.tr.Adds[j].AtNS })
	return rec.tr
}

func WriteTrace(path string, tr *Trace) error {
	b, err := json.Marshal(tr)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
