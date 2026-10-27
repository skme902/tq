// Command ours replays a parity script against github.com/skme902/tq.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/parity/driver"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/sqlite"
)

type impl struct {
	e   *taskqueue.Engine
	cs  *taskqueue.MemoryQueueConfigSource
	ns  string
	rec *driver.Recorder
}

func (m *impl) key(q string) taskqueue.QueueKey { return taskqueue.QueueKey{Namespace: m.ns, Name: q} }

func (m *impl) Add(ctx context.Context, q string, id int64, pri int32, key string, w float32) (bool, error) {
	return m.e.AddTask(ctx, m.key(q), taskqueue.Task{
		Payload:  []byte{byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24), byte(id >> 32)},
		Priority: taskqueue.Priority{PriorityKey: pri, FairnessKey: key, FairnessWeight: w},
	})
}

func decode(b []byte) int64 {
	var id int64
	for i := len(b) - 1; i >= 0; i-- {
		id = id<<8 | int64(b[i])
	}
	return id
}

func (m *impl) Poll(ctx context.Context, q, poller string) error {
	d, err := m.e.PollTask(ctx, m.key(q), taskqueue.PollOptions{Identity: poller})
	if errors.Is(err, taskqueue.ErrNoTasks) {
		return nil
	}
	if err != nil {
		return err
	}
	m.rec.Delivered(q, decode(d.Task.Payload), poller)
	// Upstream's matching acks implicitly at RecordTaskStarted; ack immediately to match.
	if err := d.Ack(); err != nil {
		return err
	}
	return nil
}

func (m *impl) SetQueueRPS(_ context.Context, q string, rps *float32) error {
	return m.cs.SetQueueRateLimit(m.key(q), rps)
}
func (m *impl) SetKeyRPS(_ context.Context, q string, rps *float32) error {
	return m.cs.SetFairnessKeyRateLimitDefault(m.key(q), rps)
}
func (m *impl) SetWeights(_ context.Context, q string, w map[string]float32) error {
	return m.cs.UpdateFairnessWeightOverrides(m.key(q), w, nil)
}

func main() {
	scriptPath := flag.String("script", "", "script json")
	out := flag.String("out", "", "trace json")
	storeName := flag.String("store", "mem", "mem|sqlite")
	run := flag.Int("run", 0, "run number")
	flag.Parse()
	s, err := driver.LoadScript(*scriptPath)
	if err != nil {
		log.Fatal(err)
	}
	var st store.Store
	switch *storeName {
	case "mem":
		st = memstore.New()
	case "sqlite":
		dir, _ := os.MkdirTemp("", "parity-sqlite")
		defer os.RemoveAll(dir)
		ss, err := sqlite.Open(context.Background(), filepath.Join(dir, "tq.db"), sqlite.Options{})
		if err != nil {
			log.Fatal(err)
		}
		if err := ss.Migrate(context.Background()); err != nil {
			log.Fatal(err)
		}
		defer ss.Close()
		st = ss
	default:
		log.Fatalf("unknown store %q", *storeName)
	}
	cfg := taskqueue.DefaultConfig()
	cfg.LongPollExpirationInterval = time.Duration(s.LongPollMS) * time.Millisecond
	cs := taskqueue.NewMemoryQueueConfigSource(0)
	rec := driver.NewRecorder("ours", *storeName, 1, s.Name, *run)
	e, err := taskqueue.New(taskqueue.Options{
		Store: st, Config: cfg, QueueConfigSource: cs,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		log.Fatal(err)
	}
	tr := driver.Run(s, &impl{e: e, cs: cs, ns: s.Namespace, rec: rec}, rec)
	e.Stop()
	if err := driver.WriteTrace(*out, tr); err != nil {
		log.Fatal(err)
	}
}
