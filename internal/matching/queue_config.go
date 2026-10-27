// MemoryQueueConfigSource replaces the task queue config part of upstream's user data
// (UpdateTaskQueueConfig in service/matching/matching_engine.go; validateFairnessWeightUpdate
// and validateRateLimit in service/frontend/validators.go) and the admin dispatch-rate dynamic
// config, temporalio/temporal v1.32.1 af08a5e5. Validation and merge logic are ported;
// storage is an in-process map instead of replicated user data, and change notification is a
// synchronous callback instead of onUserDataChanged / dynamic config subscriptions.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"fmt"
	"maps"
	"sync"

	"github.com/skme902/tq/store"
)

const (
	// fairnessKeyMaxLength is upstream common/priorities fairnessKeyMaxLength.
	fairnessKeyMaxLength = 64
	// defaultMaxFairnessKeyWeightOverrides is upstream matching.maxFairnessKeyWeightOverrides.
	defaultMaxFairnessKeyWeightOverrides = 1000
)

// MemoryQueueConfigSource is a mutable, concurrency-safe QueueConfigSource. Overrides take
// effect for tasks written after the update: like upstream, the writer reads the current
// overrides for every write batch and bakes the resulting pass into the stored task, so tasks
// already in the backlog keep the pass they were written with.
type MemoryQueueConfigSource struct {
	mu           sync.RWMutex
	maxOverrides int
	cfgs         map[store.QueueKey]QueueConfig
	nsRates      map[string]float64

	watchMu  sync.Mutex
	watchSeq int
	watchers map[store.QueueKey]map[int]func()
}

var (
	_ QueueConfigSource  = (*MemoryQueueConfigSource)(nil)
	_ QueueConfigWatcher = (*MemoryQueueConfigSource)(nil)
)

// NewMemoryQueueConfigSource returns an empty source. maxOverrides <= 0 means the upstream
// default (1000, matching.maxFairnessKeyWeightOverrides).
func NewMemoryQueueConfigSource(maxOverrides int) *MemoryQueueConfigSource {
	if maxOverrides <= 0 {
		maxOverrides = defaultMaxFairnessKeyWeightOverrides
	}
	return &MemoryQueueConfigSource{
		maxOverrides: maxOverrides,
		cfgs:         make(map[store.QueueKey]QueueConfig),
		nsRates:      make(map[string]float64),
		watchers:     make(map[store.QueueKey]map[int]func()),
	}
}

// QueueConfig implements QueueConfigSource. The returned map and pointers must not be
// modified. A per-namespace admin rate (SetAdminNamespaceDispatchRate) fills
// AdminNamespaceDispatchRate.
func (s *MemoryQueueConfigSource) QueueConfig(queue store.QueueKey) QueueConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg := s.cfgs[queue]
	if r, ok := s.nsRates[queue.Namespace]; ok {
		cfg.AdminNamespaceDispatchRate = &r
	}
	return cfg
}

// WatchQueueConfig implements QueueConfigWatcher. onChange is called synchronously, without
// locks held, after every update that affects queue.
func (s *MemoryQueueConfigSource) WatchQueueConfig(queue store.QueueKey, onChange func()) (cancel func()) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	s.watchSeq++
	id := s.watchSeq
	if s.watchers[queue] == nil {
		s.watchers[queue] = make(map[int]func())
	}
	s.watchers[queue][id] = onChange
	return func() {
		s.watchMu.Lock()
		defer s.watchMu.Unlock()
		delete(s.watchers[queue], id)
		if len(s.watchers[queue]) == 0 {
			delete(s.watchers, queue)
		}
	}
}

func (s *MemoryQueueConfigSource) notify(match func(store.QueueKey) bool) {
	s.watchMu.Lock()
	var fns []func()
	for q, ws := range s.watchers {
		if match(q) {
			for _, fn := range ws {
				fns = append(fns, fn)
			}
		}
	}
	s.watchMu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (s *MemoryQueueConfigSource) notifyQueue(queue store.QueueKey) {
	s.notify(func(q store.QueueKey) bool { return q == queue })
}

func validateRateLimit(rps *float32, label string) error {
	if rps != nil && *rps < 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf(
			"RequestsPerSecond for %s rate limit must be non-negative.", label)}
	}
	return nil
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// SetQueueRateLimit sets (or with nil, unsets) the API whole-queue rate limit in tasks/s
// (upstream UpdateTaskQueueConfig update_queue_rate_limit). Negative rates are rejected.
func (s *MemoryQueueConfigSource) SetQueueRateLimit(queue store.QueueKey, rps *float32) error {
	if err := validateRateLimit(rps, "queue"); err != nil {
		return err
	}
	s.update(queue, func(cfg *QueueConfig) { cfg.RateLimit = clonePtr(rps) })
	return nil
}

// SetFairnessKeyRateLimitDefault sets (or with nil, unsets) the per-fairness-key rate in
// tasks/s (upstream update_fairness_key_rate_limit_default). Negative rates are rejected.
func (s *MemoryQueueConfigSource) SetFairnessKeyRateLimitDefault(queue store.QueueKey, rps *float32) error {
	if err := validateRateLimit(rps, "fairness key"); err != nil {
		return err
	}
	s.update(queue, func(cfg *QueueConfig) { cfg.FairnessKeyRateLimitDefault = clonePtr(rps) })
	return nil
}

// SetAdminQueueDispatchRate overrides Config.AdminNamespaceTaskQueueToPartitionDispatchRate
// for queue (nil restores the Config value).
func (s *MemoryQueueConfigSource) SetAdminQueueDispatchRate(queue store.QueueKey, rps *float64) {
	s.update(queue, func(cfg *QueueConfig) { cfg.AdminQueueDispatchRate = clonePtr(rps) })
}

// SetAdminNamespaceDispatchRate overrides Config.AdminNamespaceToPartitionDispatchRate for
// every queue whose QueueKey.Namespace is ns (nil restores the Config value). It is applied
// per partition, like upstream; it is not a shared namespace budget.
func (s *MemoryQueueConfigSource) SetAdminNamespaceDispatchRate(ns string, rps *float64) {
	s.mu.Lock()
	if rps == nil {
		delete(s.nsRates, ns)
	} else {
		s.nsRates[ns] = *rps
	}
	s.mu.Unlock()
	s.notify(func(q store.QueueKey) bool { return q.Namespace == ns })
}

func (s *MemoryQueueConfigSource) update(queue store.QueueKey, f func(*QueueConfig)) {
	s.mu.Lock()
	cfg := s.cfgs[queue]
	f(&cfg)
	s.cfgs[queue] = cfg
	s.mu.Unlock()
	s.notifyQueue(queue)
}

// UpdateFairnessWeightOverrides upserts set and deletes unset for queue, with upstream's
// validation (non-empty keys of at most 64 bytes, weights > 0, no key in both lists,
// len(set)+len(unset) <= max) and merge/limit semantics (ErrFairnessOverridesUpdateRejected
// if the merged map would exceed max).
func (s *MemoryQueueConfigSource) UpdateFairnessWeightOverrides(queue store.QueueKey, set map[string]float32, unset []string) error {
	if err := validateFairnessWeightUpdate(set, unset, s.maxOverrides); err != nil {
		return err
	}
	s.mu.Lock()
	cfg := s.cfgs[queue]
	// Clone set so later caller mutations don't leak in (merge may return it as-is).
	merged, err := mergeFairnessWeightOverrides(cfg.FairnessWeightOverrides, maps.Clone(set), unset, s.maxOverrides)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	cfg.FairnessWeightOverrides = merged
	s.cfgs[queue] = cfg
	s.mu.Unlock()
	s.notifyQueue(queue)
	return nil
}

func validateFairnessWeightUpdate(set map[string]float32, unset []string, maxConfigLimit int) error {
	total := len(set) + len(unset)
	if total > maxConfigLimit {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf(
			"too many fairness weight overrides in request: got %d, maximum %d", total, maxConfigLimit)}
	}
	for k, w := range set {
		if k == "" {
			return &store.InvalidArgumentError{Msg: "fairness weight override key must not be empty"}
		}
		if len(k) > fairnessKeyMaxLength {
			return &store.InvalidArgumentError{Msg: "fairness key length exceeds limit"}
		}
		if w <= 0 {
			return &store.InvalidArgumentError{Msg: fmt.Sprintf(
				"invalid fairness weight weight for key %q: must be greater than zero", k)}
		}
	}
	for _, k := range unset {
		if k == "" {
			return &store.InvalidArgumentError{Msg: "fairness weight override key must not be empty"}
		}
		if len(k) > fairnessKeyMaxLength {
			return &store.InvalidArgumentError{Msg: "fairness key length exceeds limit"}
		}
	}
	for _, u := range unset {
		if _, ok := set[u]; ok {
			return &store.InvalidArgumentError{Msg: fmt.Sprintf(
				"fairness weight override key %q present in both set and unset lists", u)}
		}
	}
	return nil
}
