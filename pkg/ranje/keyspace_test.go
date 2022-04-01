package ranje

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FakePersister struct {
}

func (fp *FakePersister) GetRanges() ([]*Range, error) {
	return []*Range{}, nil
}

func (fp *FakePersister) PutRanges([]*Range) error {
	return nil
}

func Get(t *testing.T, ks *Keyspace, rID uint64) *Range {
	r, err := ks.Get(Ident(rID))
	require.NoError(t, err)
	return r
}

func TestInitial(t *testing.T) {
	ks := New(&FakePersister{})
	assert.Equal(t, 1, ks.Len())
	assert.Equal(t, "{1 [-inf, +inf] Pending}", ks.LogString())

	r := Get(t, ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ZeroKey, r.Meta.End, "range should end at ZeroKey")
}

func TestNewWithSplits(t *testing.T) {
	ks := NewWithSplits(&FakePersister{}, []string{"a", "b", "c"})
	assert.Equal(t, 4, ks.Len())
	assert.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Pending} {3 (b, c] Pending} {4 (c, +inf] Pending}", ks.LogString())
}

func TestHappyPath(t *testing.T) {
	ks := NewWithSplits(&FakePersister{}, []string{"a", "b", "c"})
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Pending} {3 (b, c] Pending} {4 (c, +inf] Pending}", ks.LogString())

	// Join

	r2 := Get(t, ks, 2)

	require.NoError(t, ks.RangeToState(r2, Placing))
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Placing} {3 (b, c] Pending} {4 (c, +inf] Pending}", ks.LogString())
	// ---------------------------------------------- ^^^^^^^

	require.NoError(t, ks.RangeToState(r2, Ready))
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Ready} {3 (b, c] Pending} {4 (c, +inf] Pending}", ks.LogString())

	r3 := Get(t, ks, 3)

	require.NoError(t, ks.RangeToState(r3, Placing))
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Ready} {3 (b, c] Placing} {4 (c, +inf] Pending}", ks.LogString())

	require.NoError(t, ks.RangeToState(r3, Ready))
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Ready} {3 (b, c] Ready} {4 (c, +inf] Pending}", ks.LogString())

	r5, err := ks.JoinTwo(r2, r3)
	require.NoError(t, err)
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Joining} {3 (b, c] Joining} {4 (c, +inf] Pending} {5 (a, c] Pending}", ks.LogString())

	require.NoError(t, ks.RangeToState(r5, Placing))
	require.NoError(t, ks.RangeToState(r5, Ready))
	require.NoError(t, ks.RangeToState(r2, Obsolete))
	require.NoError(t, ks.RangeToState(r3, Obsolete))
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, b] Obsolete} {3 (b, c] Obsolete} {4 (c, +inf] Pending} {5 (a, c] Ready}", ks.LogString())

	require.NoError(t, ks.Discard(r2))
	require.NoError(t, ks.Discard(r3))
	require.Equal(t, "{1 [-inf, a] Pending} {4 (c, +inf] Pending} {5 (a, c] Ready}", ks.LogString())

	// Split

	r1 := Get(t, ks, 1)
	require.NoError(t, ks.RangeToState(r1, Placing))
	require.NoError(t, ks.RangeToState(r1, Ready))
	require.Equal(t, "{1 [-inf, a] Ready} {4 (c, +inf] Pending} {5 (a, c] Ready}", ks.LogString())

	require.NoError(t, ks.DoSplit(r1, "1"))
	require.Equal(t, "{1 [-inf, a] Splitting} {4 (c, +inf] Pending} {5 (a, c] Ready} {6 [-inf, 1] Pending} {7 (1, a] Pending}", ks.LogString())

	r6 := Get(t, ks, 6)
	require.NoError(t, ks.RangeToState(r6, Placing))
	require.NoError(t, ks.RangeToState(r6, Ready))
	require.Equal(t, "{1 [-inf, a] Splitting} {4 (c, +inf] Pending} {5 (a, c] Ready} {6 [-inf, 1] Ready} {7 (1, a] Pending}", ks.LogString())

	r7 := Get(t, ks, 7)
	require.NoError(t, ks.RangeToState(r7, Placing))
	require.NoError(t, ks.RangeToState(r7, Ready))
	require.NoError(t, ks.RangeToState(r1, Obsolete))
	require.NoError(t, ks.RangeToState(r1, Obsolete))
	require.Equal(t, "{1 [-inf, a] Obsolete} {4 (c, +inf] Pending} {5 (a, c] Ready} {6 [-inf, 1] Ready} {7 (1, a] Ready}", ks.LogString())

	require.NoError(t, ks.Discard(r1))
	require.Equal(t, "{4 (c, +inf] Pending} {5 (a, c] Ready} {6 [-inf, 1] Ready} {7 (1, a] Ready}", ks.LogString())
}

func TestSplit(t *testing.T) {
	ks := New(&FakePersister{})
	assert.Equal(t, "{1 [-inf, +inf] Pending}", ks.LogString())
	r1 := Get(t, ks, 1)

	// The zero key (infinity) can only be the outer edges of the range.
	err := ks.DoSplit(r1, ZeroKey)
	assert.EqualError(t, err, "can't split on zero key")

	// Ranges start life as PENDING, and can't be split until they are READY.
	err = ks.DoSplit(r1, "a")
	assert.EqualError(t, err, "can't split non-ready range")

	// Mark as READY, so the split can proceed. In production this would be
	// called by some other component after receiving the ack from the node(s)
	// assigned the ranges.
	assert.NoError(t, ks.RangeToState(r1, Placing))
	assert.NoError(t, ks.RangeToState(r1, Ready))

	// Split one range results in THREE ranges.
	err = ks.DoSplit(r1, "a")
	assert.NoError(t, err)
	assert.Equal(t, "{1 [-inf, +inf] Splitting} {2 [-inf, a] Pending} {3 (a, +inf] Pending}", ks.LogString())

	// Trying to split the same range again should fail.
	err = ks.DoSplit(r1, "a")
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't split non-ready range")
	}

	// Discarding the parent fails because the children aren't READY yet.
	err = ks.Discard(r1)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't discard non-obsolete range")
	}

	xa := Get(t, ks, 2) // [-inf, a]
	ax := Get(t, ks, 3) // (a, +inf]

	// Mark the two new ranges as READY, so the predecesor becomes OBSOLETE and
	// can be discarded.
	for _, rr := range []*Range{xa, ax} {
		assert.NoError(t, ks.RangeToState(rr, Placing))
		assert.NoError(t, ks.RangeToState(rr, Ready))
	}

	ks.RangeToState(r1, Obsolete)

	// Discarding should succeed this time, leaving TWO ranges.
	err = ks.Discard(r1)
	assert.NoError(t, err)
	assert.Equal(t, 2, ks.Len())
	assert.Equal(t, "{2 [-inf, a] Ready} {3 (a, +inf] Ready}", ks.LogString())

	// Various non-sensical things also do not work.
	// TODO: Move these into the Range test?

	err = ks.DoSplit(xa, "b")
	assert.EqualError(t, err, "range R{2 [-inf, a] Ready} does not contain key: b")

	err = ks.DoSplit(xa, xa.Meta.End)
	assert.EqualError(t, err, "range R{2 [-inf, a] Ready} does not contain key: a")

	err = ks.DoSplit(ax, ax.Meta.Start)
	assert.EqualError(t, err, "range R{3 (a, +inf] Ready} starts with key: a")
}

func TestJoin(t *testing.T) {
	ks := NewWithSplits(&FakePersister{}, []string{"a"})
	require.Equal(t, "{1 [-inf, a] Pending} {2 (a, +inf] Pending}", ks.LogString())
	require.Equal(t, 2, ks.Len())

	xa := Get(t, ks, 1) // [-inf, a]
	ax := Get(t, ks, 2) // (a, +inf]

	// Joining fails, because both ranges are still pending.
	xx, err := ks.JoinTwo(xa, ax)
	require.EqualError(t, err, "can't join non-ready ranges")
	require.Nil(t, xx)

	// Mark them as ready so the join can proceed.
	require.NoError(t, ks.RangeToState(xa, Placing))
	require.NoError(t, ks.RangeToState(xa, Ready))
	require.NoError(t, ks.RangeToState(ax, Placing))
	require.NoError(t, ks.RangeToState(ax, Ready))

	// Joining succeeds, resulting in THREE ranges.
	xx, err = ks.JoinTwo(xa, ax)
	require.NoError(t, err)
	require.Equal(t, "{1 [-inf, a] Joining} {2 (a, +inf] Joining} {3 [-inf, +inf] Pending}", ks.LogString())

	// Discarding any of the parents fails because the child isn't ready yet.
	for _, r := range []*Range{xa, ax} {
		err = ks.Discard(r)
		require.Error(t, err)
		require.EqualError(t, err, "can't discard non-obsolete range")
	}

	// The child becomes ready.
	require.NoError(t, ks.RangeToState(xx, Placing))
	require.NoError(t, ks.RangeToState(xx, Ready))

	// Discarding works this time, leaving only the child.
	for _, r := range []*Range{xa, ax} {
		require.NoError(t, ks.RangeToState(r, Obsolete))
		require.NoError(t, ks.Discard(r))
	}
	require.Equal(t, 1, ks.Len())
	require.Equal(t, "{3 [-inf, +inf] Ready}", ks.LogString())
}

func TestDiscard(t *testing.T) {
	ks := New(&FakePersister{})
	r1 := Get(t, ks, 1)
	err := ks.Discard(r1)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "can't discard non-obsolete range")
	}
}
