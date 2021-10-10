package ranje

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitial(t *testing.T) {
	ks := New()
	assert.Equal(t, 1, ks.Len())
	assert.Equal(t, "{0 Pending [-inf, +inf]}", ks.Dump())

	r := ks.Get(0)
	assert.NotNil(t, r)
	assert.Equal(t, ZeroKey, r.start, "range should start at ZeroKey")
	assert.Equal(t, ZeroKey, r.end, "range should end at ZeroKey")
}

func TestNewWithSplits(t *testing.T) {
	ks := NewWithSplits([]string{"a", "b", "c"})
	assert.Equal(t, 4, ks.Len())
	assert.Equal(t, "{0 Pending [-inf, a]} {1 Pending (a, b]} {2 Pending (b, c]} {3 Pending (c, +inf]}", ks.Dump())
}

func TestHappyPath(t *testing.T) {
	ks := NewWithSplits([]string{"a", "b", "c"})
	require.Equal(t, "{0 Pending [-inf, a]} {1 Pending (a, b]} {2 Pending (b, c]} {3 Pending (c, +inf]}", ks.Dump())

	// Join

	r1 := ks.Get(1)
	require.NoError(t, r1.State(Ready))
	require.Equal(t, "{0 Pending [-inf, a]} {1 Ready (a, b]} {2 Pending (b, c]} {3 Pending (c, +inf]}", ks.Dump())

	r2 := ks.Get(2)
	require.NoError(t, r2.State(Ready))
	require.Equal(t, "{0 Pending [-inf, a]} {1 Ready (a, b]} {2 Ready (b, c]} {3 Pending (c, +inf]}", ks.Dump())

	r4, err := ks.JoinTwo(r1, r2)
	require.NoError(t, err)
	require.Equal(t, "{0 Pending [-inf, a]} {1 Joining (a, b]} {2 Joining (b, c]} {3 Pending (c, +inf]} {4 Pending (a, c]}", ks.Dump())

	require.NoError(t, r4.State(Ready))
	require.Equal(t, "{0 Pending [-inf, a]} {1 Obsolete (a, b]} {2 Obsolete (b, c]} {3 Pending (c, +inf]} {4 Ready (a, c]}", ks.Dump())

	require.NoError(t, ks.Discard(r1))
	require.NoError(t, ks.Discard(r2))
	require.Equal(t, "{0 Pending [-inf, a]} {3 Pending (c, +inf]} {4 Ready (a, c]}", ks.Dump())

	// Split

	r0 := ks.Get(0)
	require.NoError(t, r0.State(Ready))
	require.Equal(t, "{0 Ready [-inf, a]} {3 Pending (c, +inf]} {4 Ready (a, c]}", ks.Dump())

	require.NoError(t, ks.DoSplit(r0, "1"))
	require.Equal(t, "{0 Splitting [-inf, a]} {3 Pending (c, +inf]} {4 Ready (a, c]} {5 Pending [-inf, 1]} {6 Pending (1, a]}", ks.Dump())

	r5 := ks.Get(5)
	require.NoError(t, r5.State(Ready))
	require.Equal(t, "{0 Splitting [-inf, a]} {3 Pending (c, +inf]} {4 Ready (a, c]} {5 Ready [-inf, 1]} {6 Pending (1, a]}", ks.Dump())

	r6 := ks.Get(6)
	require.NoError(t, r6.State(Ready))
	require.Equal(t, "{0 Obsolete [-inf, a]} {3 Pending (c, +inf]} {4 Ready (a, c]} {5 Ready [-inf, 1]} {6 Ready (1, a]}", ks.Dump())

	require.NoError(t, ks.Discard(r0))
	require.Equal(t, "{3 Pending (c, +inf]} {4 Ready (a, c]} {5 Ready [-inf, 1]} {6 Ready (1, a]}", ks.Dump())
}

func TestSplit(t *testing.T) {
	ks := New()
	assert.Equal(t, "{0 Pending [-inf, +inf]}", ks.Dump())
	r0 := ks.Get(0)

	// The zero key (infinity) can only be the outer edges of the range.
	err := ks.DoSplit(r0, ZeroKey)
	assert.EqualError(t, err, "can't split on zero key")

	// Ranges start life as PENDING, and can't be split until they are READY.
	err = ks.DoSplit(r0, "a")
	assert.EqualError(t, err, "can't split non-ready range")

	// Mark as READY, so the split can proceed. In production this would be
	// called by some other component after receiving the ack from the node(s)
	// assigned the ranges.
	assert.NoError(t, r0.State(Ready))

	// Split one range results in THREE ranges.
	err = ks.DoSplit(r0, "a")
	assert.NoError(t, err)
	assert.Equal(t, "{0 Splitting [-inf, +inf]} {1 Pending [-inf, a]} {2 Pending (a, +inf]}", ks.Dump())

	// Trying to split the same range again should fail.
	err = ks.DoSplit(r0, "a")
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't split non-ready range")
	}

	// Discarding the parent fails because the children aren't READY yet.
	err = ks.Discard(r0)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't discard non-obsolete range")
	}

	// Mark the two new ranges as READY, so the predecesor becomes OBSOLETE and
	// can be discarded.
	for _, rr := range r0.children {
		assert.NoError(t, rr.State(Ready))
	}

	// Discarding should succeed this time, leaving TWO ranges.
	err = ks.Discard(r0)
	assert.NoError(t, err)
	assert.Equal(t, 2, ks.Len())
	assert.Equal(t, "{1 Ready [-inf, a]} {2 Ready (a, +inf]}", ks.Dump())

	// Various non-sensical things also do not work.
	// TODO: Move these into the Range test?

	xa := ks.Get(1) // [-inf, a]
	ax := ks.Get(2) // (a, +inf]

	err = ks.DoSplit(xa, "b")
	assert.EqualError(t, err, "range {1 Ready [-inf, a]} does not contain key: b")

	err = ks.DoSplit(xa, xa.end)
	assert.EqualError(t, err, "range {1 Ready [-inf, a]} does not contain key: a")

	err = ks.DoSplit(ax, ax.start)
	assert.EqualError(t, err, "range {2 Ready (a, +inf]} starts with key: a")
}

func TestJoin(t *testing.T) {
	ks := NewWithSplits([]string{"a"})
	require.Equal(t, "{0 Pending [-inf, a]} {1 Pending (a, +inf]}", ks.Dump())
	require.Equal(t, 2, ks.Len())

	xa := ks.Get(0) // [-inf, a]
	ax := ks.Get(1) // (a, +inf]

	// Joining fails, because both ranges are still pending.
	xx, err := ks.JoinTwo(xa, ax)
	require.EqualError(t, err, "can't join non-ready ranges")
	require.Nil(t, xx)

	// Mark them as ready so the join can proceed.
	require.NoError(t, xa.State(Ready))
	require.NoError(t, ax.State(Ready))

	// Joining succeeds, resulting in THREE ranges.
	xx, err = ks.JoinTwo(xa, ax)
	require.NoError(t, err)
	require.Equal(t, "{0 Joining [-inf, a]} {1 Joining (a, +inf]} {2 Pending [-inf, +inf]}", ks.Dump())

	// Discarding any of the parents fails because the child isn't ready yet.
	for _, r := range []*Range{xa, ax} {
		err = ks.Discard(r)
		require.Error(t, err)
		require.EqualError(t, err, "can't discard non-obsolete range")
	}

	// The child becomes ready.
	require.NoError(t, xx.State(Ready))

	// Discarding works this time, leaving only the child.
	for _, r := range []*Range{xa, ax} {
		require.NoError(t, ks.Discard(r))
	}
	require.Equal(t, 1, ks.Len())
	require.Equal(t, "{2 Ready [-inf, +inf]}", ks.Dump())
}

func TestDiscard(t *testing.T) {
	ks := New()
	r := ks.Get(0)
	err := ks.Discard(r)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't discard non-obsolete range")
	}
}
