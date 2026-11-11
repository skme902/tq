package matching

// StoreQueueConfigSource is a library addition: a QueueConfigSource persisted in a
// store.QueueConfigStore, shared by every process on the same database, with an audit trail
// (upstream keeps TaskQueueConfig in task queue user data with update_identity, reason and
// update_time). See README "Persisted queue config".

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
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
	// Err is set (and Config is zero) when this version's stored blob does not decode.
	Err error
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

	pollMu       sync.Mutex // guards the two fields below; pollCond signals their changes
	pollCond     *sync.Cond
	pollStopped  bool // the poll goroutine has returned
	pollNotifies bool // the poll goroutine is running watcher callbacks
}

// NewStoreQueueConfigSource loads every stored config (it fails if the store does) and
// starts polling for changes until Close. ctx bounds only that initial load; the poller runs
// until Close.
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
	}
	s.pollCond = sync.NewCond(&s.pollMu)
	if err := s.loadCurrent(ctx); err != nil {
		return nil, err
	}
	pollCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.poll(pollCtx)
	return s, nil
}

// Close stops polling. The cache keeps serving the last values.
//
// Close may be called from a watcher callback (review config r2 P1): while the poll goroutine
// is running callbacks, Close returns once polling is cancelled instead of waiting for the poll
// goroutine, which then exits after the callbacks return.
func (s *StoreQueueConfigSource) Close() {
	s.cancel()
	s.pollMu.Lock()
	for !s.pollStopped && !s.pollNotifies {
		s.pollCond.Wait()
	}
	s.pollMu.Unlock()
}

func (s *StoreQueueConfigSource) setPollState(f func()) {
	s.pollMu.Lock()
	f()
	s.pollMu.Unlock()
	s.pollCond.Broadcast()
}

func (s *StoreQueueConfigSource) poll(ctx context.Context) {
	defer s.setPollState(func() { s.pollStopped = true })
	for {
		ch, timer := s.opts.TimeSource.NewTimer(s.opts.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-ch:
		}
		if err := s.refresh(ctx, true); err != nil && ctx.Err() == nil {
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

// Refresh reads every change since the last one seen and installs, per queue, only the newest
// version: like upstream's user-data load, a peer that missed several changes moves straight
// to the latest one and notifies once, so a superseded value (say a resume followed by a new
// pause) never reaches the matcher. A newest version that does not decode is logged and
// skipped; older rows of that queue are not replayed instead.
func (s *StoreQueueConfigSource) Refresh(ctx context.Context) error {
	return s.refresh(ctx, false)
}

func (s *StoreQueueConfigSource) refresh(ctx context.Context, onPoller bool) error {
	const page = 500
	s.mu.Lock()
	after := s.seq
	s.mu.Unlock()
	newest := make(map[store.QueueKey]store.QueueConfigRecord)
	for {
		recs, err := s.st.ListQueueConfigChanges(ctx, after, page)
		if err != nil {
			return err
		}
		for _, r := range recs {
			after = max(after, r.Seq)
			if r.Version > newest[r.Key].Version {
				newest[r.Key] = r
			}
		}
		if len(recs) < page {
			break
		}
	}
	var changed []store.QueueKey
	for _, r := range newest {
		if s.install(r.Key, r.Version, s.resolve(ctx, r)) {
			changed = append(changed, r.Key)
		}
	}
	s.mu.Lock()
	s.seq = max(s.seq, after) // before the callbacks: a stuck callback does not hold back the feed
	s.mu.Unlock()
	if onPoller && len(changed) > 0 {
		s.setPollState(func() { s.pollNotifies = true })
		defer s.setPollState(func() { s.pollNotifies = false })
	}
	for _, k := range changed {
		s.notify(k)
	}
	return nil
}

// loadCurrent installs every queue's current config and moves the feed position to the
// newest of them, so startup reads one row per queue instead of the whole history (which the
// library never trims); polling then reads only later changes.
func (s *StoreQueueConfigSource) loadCurrent(ctx context.Context) error {
	recs, err := s.st.ListCurrentQueueConfigs(ctx)
	if err != nil {
		return err
	}
	var seq int64
	for _, r := range recs {
		seq = max(seq, r.Seq)
		s.install(r.Key, r.Version, s.resolve(ctx, r)) // no watchers yet
	}
	s.mu.Lock()
	s.seq = max(s.seq, seq)
	s.mu.Unlock()
	return nil
}

// resolve returns the config a stored version stands for. A version that does not decode or
// fails validation (written around Update) stands for the newest earlier version of that
// queue that passes both, or the default config if none does; it is installed at its own
// version, so a stale page cannot bring back what it replaced (review config r2 P0, P2).
func (s *StoreQueueConfigSource) resolve(ctx context.Context, r store.QueueConfigRecord) QueueConfig {
	cfg, err := s.decodeValid(r.Config)
	if err == nil {
		return cfg
	}
	s.opts.Metrics.Counter(metrics.QueueConfigRefreshFailures, 1)
	s.opts.Logger.Error("unusable queue config version; using the newest earlier valid one",
		"queue", r.Key.String(), "version", r.Version, "error", err)
	hist, herr := s.st.QueueConfigHistory(ctx, r.Key, 100)
	if herr != nil {
		s.opts.Logger.Error("queue config history read failed", "queue", r.Key.String(), "error", herr)
	}
	for _, h := range hist { // newest first
		if h.Version < r.Version {
			if c, err := s.decodeValid(h.Config); err == nil {
				return c
			}
		}
	}
	return QueueConfig{}
}

func (s *StoreQueueConfigSource) decodeValid(b []byte) (QueueConfig, error) {
	cfg, err := decodeQueueConfig(b)
	if err != nil {
		return QueueConfig{}, err
	}
	if err := validateQueueConfig(cfg, s.mem.maxOverrides); err != nil {
		return QueueConfig{}, err
	}
	return cfg, nil
}

// install caches cfg as the queue's config if version is newer than the cached one, and
// reports whether the cached config changed; the caller then notifies (notify), with no lock
// of this source held, so one queue's notification does not block other queues.
func (s *StoreQueueConfigSource) install(key store.QueueKey, version int64, cfg QueueConfig) (changed bool) {
	s.mu.Lock()
	if version <= s.version[key] {
		s.mu.Unlock()
		return false
	}
	s.version[key] = version
	s.mem.mu.Lock()
	same := reflect.DeepEqual(s.mem.cfgs[key], cfg)
	s.mem.cfgs[key] = cfg
	s.mem.mu.Unlock()
	s.mu.Unlock()
	return !same // an unchanged rewrite does not make loaded queues reprocess (review config r2 P1)
}

// notify runs the queue's watchers with no lock of this source held: a watcher may call back
// into the source (QueueConfig, Update, Refresh, Close). A panicking watcher is logged and
// counted as a refresh failure instead of killing the poll goroutine.
func (s *StoreQueueConfigSource) notify(key store.QueueKey) {
	defer func() {
		if p := recover(); p != nil {
			s.opts.Metrics.Counter(metrics.QueueConfigRefreshFailures, 1)
			s.opts.Logger.Error("queue config watcher panicked", "queue", key.String(), "panic", p)
		}
	}()
	s.mem.notifyQueue(key)
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
			curVersion = rec.Version
			// A bad current blob is replaced; mutate starts from what it stands for (the
			// newest earlier valid version), the same config every process is serving.
			cur = s.resolve(ctx, *rec)
		case !store.IsNotFound(err):
			return QueueConfig{}, err
		}
		next := cloneQueueConfig(cur)
		if err := mutate(&next); err != nil {
			return QueueConfig{}, err
		}
		if len(next.PausedFairnessKeys) > 0 { // same normal form as SetFairnessKeyPaused
			next.PausedFairnessKeys = slices.Compact(slices.Sorted(slices.Values(next.PausedFairnessKeys)))
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
		// Cache and return fresh decodes of what was written, so nothing the mutate callback
		// still references aliases the live config.
		cached, err := decodeQueueConfig(blob)
		if err != nil {
			return QueueConfig{}, err
		}
		if s.install(queue, out.Version, cached) { // the feed position stays: it may hold others' writes
			s.notify(queue)
		}
		return decodeQueueConfig(blob)
	}
	return QueueConfig{}, lastErr
}

// History returns up to limit versions of the queue's config, newest first.
func (s *StoreQueueConfigSource) History(ctx context.Context, queue store.QueueKey, limit int) ([]QueueConfigVersion, error) {
	recs, err := s.st.QueueConfigHistory(ctx, queue, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueueConfigVersion, 0, len(recs))
	for _, r := range recs {
		cfg, err := decodeQueueConfig(r.Config) // one bad row does not hide the others
		out = append(out, QueueConfigVersion{Queue: r.Key, Version: r.Version, Config: cfg,
			UpdatedBy: r.UpdatedBy, Reason: r.Reason, UpdateTime: r.UpdateTime, Err: err})
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
	if len(c.PausedFairnessKeys) > maxOverrides {
		errs = append(errs, &store.InvalidArgumentError{Msg: fmt.Sprintf("too many paused fairness keys: got %d, maximum %d", len(c.PausedFairnessKeys), maxOverrides)})
	}
	for _, k := range c.PausedFairnessKeys {
		if k == "" {
			errs = append(errs, &store.InvalidArgumentError{Msg: "paused fairness key must not be empty"})
		} else if len(k) > fairnessKeyMaxLength {
			errs = append(errs, &store.InvalidArgumentError{Msg: "fairness key length exceeds limit"})
		}
	}
	if c.MaxTaskAge != nil && *c.MaxTaskAge < 0 {
		errs = append(errs, &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxTaskAge must be non-negative, got %v", *c.MaxTaskAge)})
	}
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
