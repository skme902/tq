// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/fair_level_test.go
// (TestFairLevelComparator uses a tidwall/btree ordered by fairLevel.less instead of gods treemap).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"
)

func TestFairLevelLess(t *testing.T) {
	assert.True(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 10, id: 105}))
	assert.False(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 10, id: 100}))
	assert.True(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 11, id: 100}))
	assert.False(t, fairLevel{pass: 10, id: 100}.less(fairLevel{pass: 5, id: 105}))
}

func TestFairLevelComparator(t *testing.T) {
	m := btree.NewBTreeG(fairLevel.less)
	a, b, c, d := fairLevel{pass: 10, id: 105}, fairLevel{pass: 10, id: 100}, fairLevel{pass: 11, id: 100}, fairLevel{pass: 5, id: 105}
	m.Set(a)
	m.Set(b)
	m.Set(c)
	m.Set(d)
	assert.Equal(t, m.Items(), []fairLevel{d, b, a, c})
}

func TestFairLevelMax(t *testing.T) {
	assert.Equal(t, fairLevel{pass: 10, id: 105}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 10, id: 105}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 10, id: 100}))
	assert.Equal(t, fairLevel{pass: 11, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 11, id: 100}))
	assert.Equal(t, fairLevel{pass: 10, id: 100}, fairLevel{pass: 10, id: 100}.max(fairLevel{pass: 5, id: 105}))
}
