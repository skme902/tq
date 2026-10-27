// Package cache is a minimal port of temporalio/temporal v1.32.1 af08a5e5 common/cache/lru.go:
// the count-limited, non-pinned, no-TTL LRU that rateLimitManager uses for per-fairness-key
// ready times (cache.New(size, nil)). Kept: Get moves to front, Put upserts and moves to front,
// eviction from the back when over maxSize, maxSize 0 disables the cache (Get nil, Put no-op),
// Iterator walks from most to least recently used. Dropped: TTL, pinning/refcounts, SizeGetter
// (every entry has size 1, as upstream's getSize returns for values without CacheSize), metrics
// and lifecycle callbacks. Generic instead of any-typed.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.
package cache

import (
	"container/list"
	"sync"
)

// LRU is a concurrency-safe, count-limited least-recently-used cache.
type LRU[K comparable, V any] struct {
	mut      sync.Mutex
	byAccess *list.List
	byKey    map[K]*list.Element
	maxSize  int
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// Entry is one key/value pair returned by an Iterator.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// New creates an LRU holding at most maxSize entries.
func New[K comparable, V any](maxSize int) *LRU[K, V] {
	return &LRU[K, V]{
		byAccess: list.New(),
		byKey:    make(map[K]*list.Element),
		maxSize:  maxSize,
	}
}

// Get returns the value for key and marks it most recently used.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	var zero V
	if c.maxSize == 0 {
		return zero, false
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	element := c.byKey[key]
	if element == nil {
		return zero, false
	}
	c.byAccess.MoveToFront(element)
	return element.Value.(*entry[K, V]).value, true
}

// Put sets key to value, marks it most recently used and evicts the least recently used
// entries while over maxSize. It returns the previous value, if any.
func (c *LRU[K, V]) Put(key K, value V) (V, bool) {
	var zero V
	if c.maxSize == 0 {
		return zero, false
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	if elt := c.byKey[key]; elt != nil {
		e := elt.Value.(*entry[K, V])
		existing := e.value
		e.value = value
		c.byAccess.MoveToFront(elt)
		return existing, true
	}
	// tryEvictUntilEnoughSpaceWithSkipEntry: every entry has size 1.
	for c.byAccess.Len()+1 > c.maxSize {
		back := c.byAccess.Back()
		c.byAccess.Remove(back)
		delete(c.byKey, back.Value.(*entry[K, V]).key)
	}
	c.byKey[key] = c.byAccess.PushFront(&entry[K, V]{key: key, value: value})
	return zero, false
}

// Delete removes key.
func (c *LRU[K, V]) Delete(key K) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if elt := c.byKey[key]; elt != nil {
		c.byAccess.Remove(elt)
		delete(c.byKey, key)
	}
}

// Size returns the number of entries.
func (c *LRU[K, V]) Size() int {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.byAccess.Len()
}

// Entries returns a snapshot of all entries from most to least recently used. Deviation:
// upstream's Iterator holds the cache lock until Close; a snapshot avoids that and lets the
// caller Put while walking (rateLimitManager collects updates and Puts after Close anyway).
func (c *LRU[K, V]) Entries() []Entry[K, V] {
	c.mut.Lock()
	defer c.mut.Unlock()
	out := make([]Entry[K, V], 0, c.byAccess.Len())
	for e := c.byAccess.Front(); e != nil; e = e.Next() {
		en := e.Value.(*entry[K, V])
		out = append(out, Entry[K, V]{Key: en.key, Value: en.value})
	}
	return out
}
