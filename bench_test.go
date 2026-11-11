package taskqueue_test

// Throughput benchmarks per store (slice 5, not upstream). Each op is one task's full life:
// AddTask + PollTask + Ack. "backlog" writes everything before any consumer polls, so every
// task goes through CreateTasks, GetTasks and CompleteTasksLessThan; "sync" parks consumers
// first, so most tasks are handed over in memory and never touch the store.
//
//	go test -run '^$' -bench BenchmarkEngine -benchtime 5000x -count 5 .
//
// TQ_BENCH_METRICS=counting swaps the default no-op metrics handler for benchCountingMetrics, to
// measure what emitting the per-queue metrics costs when a handler actually records them.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/sqlite"
)

func benchKinds() []testdb.Kind {
	ks := testdb.Kinds()
	return append(ks[:2:2], testdb.Kind{Name: "sqlite-normal", Durable: true, NewDB: func(tb testing.TB) testdb.Opener {
		return testdb.SQLiteWith(tb, sqlite.Options{Synchronous: sqlite.SyncNormal})
	}}, ks[2])
}

var benchSeq atomic.Int64

// benchCountingMetrics is a cheap recording handler: one atomic add per call plus a pass over the
// tags, roughly what a Prometheus/OTel handler with pre-resolved series costs.
type benchCountingMetrics struct{ calls, tags atomic.Int64 }

func (c *benchCountingMetrics) record(tags []metrics.Tag) {
	n := 0
	for _, t := range tags {
		n += len(t.Key) + len(t.Value)
	}
	c.calls.Add(1)
	c.tags.Add(int64(n))
}
func (c *benchCountingMetrics) Counter(_ string, _ int64, tags ...metrics.Tag)       { c.record(tags) }
func (c *benchCountingMetrics) Gauge(_ string, _ float64, tags ...metrics.Tag)       { c.record(tags) }
func (c *benchCountingMetrics) Timer(_ string, _ time.Duration, tags ...metrics.Tag) { c.record(tags) }

func benchMetrics() (metrics.Handler, *benchCountingMetrics) {
	if os.Getenv("TQ_BENCH_METRICS") == "counting" {
		c := &benchCountingMetrics{}
		return c, c
	}
	return metrics.Noop, nil
}

// countingStore counts task rows written and read, to confirm which path a run exercised.
type countingStore struct {
	store.Store
	written, read atomic.Int64
	commits       atomic.Int64 // successful CreateTasks calls
}

func (c *countingStore) CreateTasks(ctx context.Context, r *store.CreateTasksRequest) (*store.CreateTasksResponse, error) {
	resp, err := c.Store.CreateTasks(ctx, r)
	if err == nil {
		c.written.Add(int64(len(r.Tasks)))
		c.commits.Add(1)
	}
	return resp, err
}

func (c *countingStore) GetTasks(ctx context.Context, r *store.GetTasksRequest) (*store.GetTasksResponse, error) {
	resp, err := c.Store.GetTasks(ctx, r)
	if err == nil {
		c.read.Add(int64(len(resp.Tasks)))
	}
	return resp, err
}

func BenchmarkEngine(b *testing.B) {
	const consumers = 8
	for _, k := range benchKinds() {
		b.Run(k.Name, func(b *testing.B) {
			open := k.NewDB(b)
			raw := open(b)
			for _, m := range []struct {
				mode      string
				producers int
			}{{"backlog", 8}, {"backlog", 64}, {"sync", 8}} {
				mode, producers := m.mode, m.producers
				b.Run(fmt.Sprintf("%s-p%d", mode, producers), func(b *testing.B) {
					st := &countingStore{Store: raw}
					mh, counting := benchMetrics()
					e, err := taskqueue.New(taskqueue.Options{Store: st, Metrics: mh})
					if err != nil {
						b.Fatal(err)
					}
					defer e.Stop()
					qk := store.QueueKey{Namespace: "bench", Name: fmt.Sprint(mode, benchSeq.Add(1))}
					ctx := context.Background()
					// Warm up: load the queue (takeover) and the connection pools.
					if _, err := e.AddTask(ctx, qk, task("warm")); err != nil {
						b.Fatal(err)
					}
					d, err := e.PollTask(ctx, qk, taskqueue.PollOptions{})
					if err != nil {
						b.Fatal(err)
					}
					_ = d.Ack()

					total := int64(b.N)
					seen := make([]atomic.Bool, b.N+1)
					var acked, syncMatched, dups atomic.Int64
					var cwg, pwg sync.WaitGroup
					consume := func() {
						for range consumers {
							cwg.Go(func() {
								for acked.Load() < total {
									cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
									d, err := e.PollTask(cctx, qk, taskqueue.PollOptions{})
									cancel()
									if err != nil {
										continue // ErrNoTasks / timeout: re-check the count
									}
									if d.SyncMatched {
										syncMatched.Add(1)
									}
									var id int
									fmt.Sscan(string(d.Task.Payload), &id)
									if seen[id].Swap(true) {
										dups.Add(1)
									}
									if err := d.Ack(); err != nil {
										b.Error(err)
									}
									acked.Add(1)
								}
							})
						}
					}
					produce := func() {
						var next atomic.Int64
						for range producers {
							pwg.Go(func() {
								for i := next.Add(1); i <= total; i = next.Add(1) {
									for {
										_, err := e.AddTask(ctx, qk, task(fmt.Sprint(i)))
										if errors.Is(err, taskqueue.ErrResourceExhausted) {
											time.Sleep(100 * time.Microsecond)
											continue
										}
										if err != nil {
											b.Error(err)
										}
										break
									}
								}
							})
						}
					}
					w0, r0, c0 := st.written.Load(), st.read.Load(), st.commits.Load()
					b.ResetTimer()
					start := time.Now()
					var wrote time.Duration
					if mode == "sync" {
						consume()
						time.Sleep(20 * time.Millisecond) // let consumers park (excluded below)
						start = time.Now()
						produce()
					} else {
						produce()
						pwg.Wait()
						wrote = time.Since(start)
						consume()
					}
					pwg.Wait()
					cwg.Wait()
					el := time.Since(start)
					b.StopTimer()
					if n := dups.Load(); n > 0 {
						b.Errorf("%d duplicate deliveries", n)
					}
					for i := 1; i <= b.N; i++ {
						if !seen[i].Load() {
							b.Fatalf("task %d never delivered", i)
						}
					}
					b.ReportMetric(float64(b.N)/el.Seconds(), "tasks/s")
					if wrote > 0 {
						b.ReportMetric(float64(b.N)/wrote.Seconds(), "write-tasks/s")
						b.ReportMetric(float64(b.N)/(el-wrote).Seconds(), "drain-tasks/s")
					}
					b.ReportMetric(float64(st.written.Load()-w0)/float64(b.N), "rows-written/task")
					b.ReportMetric(float64(st.read.Load()-r0)/float64(b.N), "rows-read/task")
					if c := st.commits.Load() - c0; c > 0 {
						b.ReportMetric(float64(st.written.Load()-w0)/float64(c), "rows/commit")
					}
					b.ReportMetric(100*float64(syncMatched.Load())/float64(b.N), "sync%")
					if counting != nil {
						b.ReportMetric(float64(counting.calls.Load())/float64(b.N), "metric-calls/task")
					}
					b.ReportMetric(0, "ns/op") // wall time above is the real figure; ns/op includes the park sleep
				})
			}
		})
	}
}

// BenchmarkStoreCommit is the latency floor of one durable write: a single-task CreateTasks
// (one transaction: lock + insert + commit) from one goroutine.
func BenchmarkStoreCommit(b *testing.B) {
	for _, k := range benchKinds() {
		b.Run(k.Name, func(b *testing.B) {
			st := k.NewDB(b)(b)
			ctx := context.Background()
			qk := store.QueueKey{Namespace: "bench", Name: "commit"}
			info := &store.QueueInfo{Key: qk, Subqueues: []store.SubqueueInfo{{}}, LastUpdateTime: time.Now()}
			if err := st.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				_, err := st.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info, Subqueues: []int{0},
					Tasks: []store.TaskInfo{{ID: int64(i + 1), Pass: 1, Data: store.Task{Payload: []byte("x")}}}})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
