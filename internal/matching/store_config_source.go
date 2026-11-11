package matching

// StoreQueueConfigSource is a library addition: a QueueConfigSource persisted in a
// store.QueueConfigStore, shared by every process on the same database, with an audit trail
// (upstream keeps TaskQueueConfig in task queue user data with update_identity, reason and
// update_time). See README "Persisted queue config".

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

var (
	_ QueueConfigSource  = (*StoreQueueConfigSource)(nil)
	_ QueueConfigWatcher = (*StoreQueueConfigSource)(nil)
)

// StoreQueueConfigSourceOptions configures NewStoreQueueConfigSource.
type StoreQueueConfigSourceOptions struct {
	// PollInterval is how often changes made by other processes are read (default 5s).
	// Changes made through this source apply locally at once.
	PollInterval time.Duration
	// MaxFairnessOverrides bounds FairnessWeightOverrides (default 1000, as upstream).
	MaxFairnessOverrides int
	TimeSource           clock.TimeSource
	Logger               *slog.Logger
	Metrics              metrics.Handler
}

// ConfigChange identifies who changed a queue's config and why (recorded in the history).
type ConfigChange struct {
	Identity string
	Reason   string
}

// QueueConfigVersion is one entry of a queue's config history.
type QueueConfigVersion struct {
	Queue      store.QueueKey
	Version    int64
	Config     QueueConfig
	UpdatedBy  string
	Reason     string
	UpdateTime time.Time
}

// StoreQueueConfigSource caches every queue's persisted config in memory, serves
// QueueConfig from the cache, and notifies watchers (loaded queues) when a version changes,
// whether written here or read from the store's change feed.
type StoreQueueConfigSource struct {
	st      store.QueueConfigStore
	opts    StoreQueueConfigSourceOptions
	mem     *MemoryQueueConfigSource // cache + watchers
	mu      sync.Mutex               // serialises applying versions and the feed position
	version map[store.QueueKey]int64
	seq     int64
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewStoreQueueConfigSource loads every stored config (it fails if the store does) and
// starts polling for changes until Close.
func NewStoreQueueConfigSource(ctx context.Context, st store.QueueConfigStore, opts StoreQueueConfigSourceOptions) (*StoreQueueConfigSource, error) {
	if st == nil {
		return nil, &store.InvalidArgumentError{Msg: "NewStoreQueueConfigSource: nil store"}
	}
	if opts.PollInterval < 0 {
		return nil, &store.InvalidArgumentError{Msg: "NewStoreQueueConfigSource: PollInterval must be non-negative"}
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 5 * time.Second
	}
	if opts.TimeSource == nil {
		opts.TimeSource = clock.NewRealTimeSource()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.Noop
	}
	s := &StoreQueueConfigSource{
		st: st, opts: opts,
		mem:     NewMemoryQueueConfigSource(opts.MaxFairnessOverrides),
		version: make(map[store.QueueKey]int64),
		done:    make(chan struct{}),
	}
	if err := s.Refresh(ctx); err != nil {
		return nil, err
	}
	pollCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.poll(pollCtx)
	return s, nil
}

// Close stops polling. The cache keeps serving the last values.
func (s *StoreQueueConfigSource) Close() {
	s.cancel()
	<-s.done
}

func (s *StoreQueueConfigSource) poll(ctx context.Context) {
	defer close(s.done)
	for {
		ch, timer := s.opts.TimeSource.NewTimer(s.opts.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-ch:
		}
		if err := s.Refresh(ctx); err != nil && ctx.Err() == nil {
			s.opts.Metrics.Counter(metrics.QueueConfigRefreshFailures, 1)
			s.opts.Logger.Warn("queue config refresh failed", "error", err)
		}
	}
}

// QueueConfig implements QueueConfigSource. The returned map and pointers must not be
// modified.
func (s *StoreQueueConfigSource) QueueConfig(queue store.QueueKey) QueueConfig {
	return s.mem.QueueConfig(queue)
}

// WatchQueueConfig implements QueueConfigWatcher.
func (s *StoreQueueConfigSource) WatchQueueConfig(queue store.QueueKey, onChange func()) (cancel func()) {
	return s.mem.WatchQueueConfig(queue, onChange)
}

// Refresh reads every change since the last one seen and applies the newer versions.
func (s *StoreQueueConfigSource) Refresh(ctx context.Context) error {
	const page = 500
	for {
		s.mu.Lock()
		after := s.seq
		s.mu.Unlock()
		recs, err := s.st.ListQueueConfigChanges(ctx, after, page)
		if err != nil {
			return err
		}
		for _, r := range recs {
			s.apply(r)
		}
		if len(recs) < page {
			return nil
		}
	}
}

// apply installs r if it is newer than the cached version and advances the feed position.
func (s *StoreQueueConfigSource) apply(r store.QueueConfigRecord) {
	s.mu.Lock()
	s.seq = max(s.seq, r.Seq)
	if r.Version <= s.version[r.Key] {
		s.mu.Unlock()
		return
	}
	cfg, err := decodeQueueConfig(r.Config)
	if err != nil {
		s.mu.Unlock()
		s.opts.Metrics.Counter(metrics.QueueConfigRefreshFailures, 1)
		s.opts.Logger.Error("ignoring undecodable queue config", "queue", r.Key.String(), "version", r.Version, "error", err)
		return
	}
	s.version[r.Key] = r.Version
	s.mem.update(r.Key, func(c *QueueConfig) { *c = cfg }) // notifies watchers (no lock held there)
	s.mu.Unlock()
}

// Update changes one queue's config: mutate gets a copy of the current stored value, and
// the result is validated and written as a new version with change's identity and reason.
// A concurrent write by another process is retried on the fresh value (up to 10 times, then
// the *store.ConditionFailedError is returned). The new value applies locally before Update
// returns and reaches other processes on their next poll.
func (s *StoreQueueConfigSource) Update(ctx context.Context, queue store.QueueKey, change ConfigChange, mutate func(*QueueConfig) error) (QueueConfig, error) {
	if change.Identity == "" {
		return QueueConfig{}, &store.InvalidArgumentError{Msg: "Update: ConfigChange.Identity is required"}
	}
	var lastErr error
	for range 10 {
		var cur QueueConfig
		var curVersion int64
		rec, err := s.st.GetQueueConfig(ctx, queue)
		switch {
		case err == nil:
			if cur, err = decodeQueueConfig(rec.Config); err != nil {
				return QueueConfig{}, err
			}
			curVersion = rec.Version
		case !store.IsNotFound(err):
			return QueueConfig{}, err
		}
		next := cloneQueueConfig(cur)
		if err := mutate(&next); err != nil {
			return QueueConfig{}, err
		}
		if err := validateQueueConfig(next, s.mem.maxOverrides); err != nil {
			return QueueConfig{}, err
		}
		blob, err := json.Marshal(next)
		if err != nil {
			return QueueConfig{}, &store.InvalidArgumentError{Msg: "encode queue config: " + err.Error()}
		}
		out, err := s.st.PutQueueConfig(ctx, &store.PutQueueConfigRequest{
			Key: queue, ExpectedVersion: curVersion, Config: blob,
			UpdatedBy: change.Identity, Reason: change.Reason, UpdateTime: s.opts.TimeSource.Now(),
		})
		if store.IsConditionFailed(err) {
			lastErr = err
			continue
		}
		if err != nil {
			return QueueConfig{}, err
		}
		s.opts.Metrics.Counter(metrics.QueueConfigUpdates, 1)
		s.opts.Logger.Info("queue config updated", "queue", queue.String(), "version", out.Version,
			"identity", change.Identity, "reason", change.Reason)
		s.applyLocal(*out, next)
		return next, nil
	}
	return QueueConfig{}, lastErr
}

// applyLocal installs a version this source wrote, without moving the feed position (the
// feed may still hold earlier writes by others).
func (s *StoreQueueConfigSource) applyLocal(r store.QueueConfigRecord, cfg QueueConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Version <= s.version[r.Key] {
		return
	}
	s.version[r.Key] = r.Version
	s.mem.update(r.Key, func(c *QueueConfig) { *c = cfg })
}

// History returns up to limit versions of the queue's config, newest first.
func (s *StoreQueueConfigSource) History(ctx context.Context, queue store.QueueKey, limit int) ([]QueueConfigVersion, error) {
	recs, err := s.st.QueueConfigHistory(ctx, queue, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueueConfigVersion, 0, len(recs))
	for _, r := range recs {
		cfg, err := decodeQueueConfig(r.Config)
		if err != nil {
			return nil, err
		}
		out = append(out, QueueConfigVersion{Queue: r.Key, Version: r.Version, Config: cfg,
			UpdatedBy: r.UpdatedBy, Reason: r.Reason, UpdateTime: r.UpdateTime})
	}
	return out, nil
}

func decodeQueueConfig(b []byte) (QueueConfig, error) {
	var cfg QueueConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return QueueConfig{}, &store.InvalidArgumentError{Msg: "decode queue config: " + err.Error()}
	}
	return cfg, nil
}

func cloneQueueConfig(c QueueConfig) QueueConfig {
	c.FairnessWeightOverrides = maps.Clone(c.FairnessWeightOverrides)
	c.RateLimit = clonePtr(c.RateLimit)
	c.FairnessKeyRateLimitDefault = clonePtr(c.FairnessKeyRateLimitDefault)
	c.AdminNamespaceDispatchRate = clonePtr(c.AdminNamespaceDispatchRate)
	c.AdminQueueDispatchRate = clonePtr(c.AdminQueueDispatchRate)
	c.MaxAttempts = clonePtr(c.MaxAttempts)
	c.MaxTaskAge = clonePtr(c.MaxTaskAge)
	c.PausedFairnessKeys = slices.Clone(c.PausedFairnessKeys)
	return c
}

// validateQueueConfig applies the MemoryQueueConfigSource setters' checks to a whole config.
func validateQueueConfig(c QueueConfig, maxOverrides int) error {
	errs := []error{
		validateRateLimit(c.RateLimit, "queue"),
		validateRateLimit(c.FairnessKeyRateLimitDefault, "fairness key"),
		validateFairnessWeightUpdate(c.FairnessWeightOverrides, nil, maxOverrides),
	}
	for _, r := range []*float64{c.AdminNamespaceDispatchRate, c.AdminQueueDispatchRate} {
		if r != nil && *r < 0 {
			errs = append(errs, &store.InvalidArgumentError{Msg: fmt.Sprintf("admin dispatch rate must be non-negative, got %v", *r)})
		}
	}
	if c.MaxAttempts != nil && *c.MaxAttempts < 0 {
		errs = append(errs, &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxAttempts must be non-negative, got %d", *c.MaxAttempts)})
	}
	if c.MaxTaskAge != nil && *c.MaxTaskAge < 0 {
		errs = append(errs, &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxTaskAge must be non-negative, got %v", *c.MaxTaskAge)})
	}
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return errors.Join() // nil
}
