// Ported from temporalio/temporal v1.32.1 af08a5e5, common/cache/lru_test.go: TestLRU (minus
// metrics assertions), TestGenerics, TestIterator (as Entries), TestZeroSizeCache (minus
// PutIfNotExist/Release, which the port does not have).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.
package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func get[K comparable, V any](c *LRU[K, V], k K) any {
	if v, ok := c.Get(k); ok {
		return v
	}
	return nil
}

func TestLRU(t *testing.T) {
	t.Parallel()
	cache := New[string, string](4)

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", get(cache, "A"))
	assert.Nil(t, get(cache, "B"))
	assert.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Equal(t, 4, cache.Size())

	assert.Equal(t, "Bar", get(cache, "B"))
	assert.Equal(t, "Cid", get(cache, "C"))
	assert.Equal(t, "Delt", get(cache, "D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", get(cache, "A"))
	assert.Equal(t, 4, cache.Size())

	cache.Put("E", "Epsi")
	assert.Equal(t, "Epsi", get(cache, "E"))
	assert.Equal(t, "Foo2", get(cache, "A"))
	assert.Nil(t, get(cache, "B")) // Oldest, should be evicted
	assert.Equal(t, 4, cache.Size())

	// Access C, D is now LRU
	cache.Get("C")
	cache.Put("F", "Felp")
	assert.Nil(t, get(cache, "D"))
	assert.Equal(t, 4, cache.Size())

	cache.Delete("A")
	assert.Nil(t, get(cache, "A"))
	assert.Equal(t, 3, cache.Size())
}

type keyType struct {
	dummyString string
	dummyInt    int
}

func TestGenerics(t *testing.T) {
	t.Parallel()

	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := New[keyType, string](5)
	cache.Put(key, value)

	assert.Equal(t, value, get(cache, key))
	assert.Equal(t, value, get(cache, keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	assert.Nil(t, get(cache, keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
	assert.Equal(t, 1, cache.Size())

	cache.Put(key, "some other random value")
	assert.Equal(t, "some other random value", get(cache, key))
	assert.Equal(t, 1, cache.Size())
}

func TestIterator(t *testing.T) {
	t.Parallel()

	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := New[string, string](5)

	for k, v := range expected {
		cache.Put(k, v)
	}

	actual := map[string]string{}
	for _, entry := range cache.Entries() {
		actual[entry.Key] = entry.Value
	}
	assert.Equal(t, expected, actual)
}

func TestZeroSizeCache(t *testing.T) {
	t.Parallel()

	cache := New[string, int](0)
	cache.Put("A", 1)
	assert.Nil(t, get(cache, "A"))
	assert.Equal(t, 0, cache.Size())
	assert.Empty(t, cache.Entries())
	cache.Delete("A")
	assert.Equal(t, 0, cache.Size())
}
