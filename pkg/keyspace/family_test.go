package keyspace

import (
	"testing"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFamily(t *testing.T) {

	//      ┌───┐
	//   ┌──│ 1 │───────┐
	//   ▼  └───┘       ▼
	// ┌───┐          ┌───┐
	// │ 2 │       ┌──│ 3 │──┐
	// └───┘       ▼  └───┘  ▼
	//   │       ┌───┐     ┌───┐
	//   │       │ 4 │     │ 5 │
	//   │       └───┘     └───┘
	//   │  ┌───┐  │         │
	//   └─▶│ 6 │◀─┘         │
	//      └───┘            │
	//        │       ┌───┐  │
	//        └──────▶│ 7 │◀─┘
	//                └───┘
	//
	// R1 is a genesis range.
	// R1 was split into R2 and R3 at key m.
	// R3 was split into R4 and R5 at key t.
	// R2 and R4 were joined into R6.
	// R5 and R6 are currently being joined into R7.
	// R7 is the only active range remaining.

	r1 := &ranje.Range{
		State:      ranje.RsObsolete,
		Children:   []ranje.Ident{2, 3},
		Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey, End: ranje.ZeroKey},
		Placements: []*ranje.Placement{},
	}

	r2 := &ranje.Range{
		State:      ranje.RsObsolete,
		Parents:    []ranje.Ident{1},
		Children:   []ranje.Ident{6},
		Meta:       ranje.Meta{Ident: 2, End: ranje.Key("m")},
		Placements: []*ranje.Placement{},
	}

	r3 := &ranje.Range{
		State:      ranje.RsObsolete,
		Parents:    []ranje.Ident{1},
		Children:   []ranje.Ident{4, 5},
		Meta:       ranje.Meta{Ident: 3, Start: ranje.Key("m")},
		Placements: []*ranje.Placement{},
	}

	r4 := &ranje.Range{
		State:      ranje.RsObsolete,
		Parents:    []ranje.Ident{3},
		Children:   []ranje.Ident{6},
		Meta:       ranje.Meta{Ident: 4, Start: ranje.Key("m"), End: ranje.Key("t")},
		Placements: []*ranje.Placement{},
	}

	r5 := &ranje.Range{
		State:      ranje.RsSubsuming,
		Parents:    []ranje.Ident{3},
		Children:   []ranje.Ident{7},
		Meta:       ranje.Meta{Ident: 5, Start: ranje.Key("t")},
		Placements: []*ranje.Placement{{NodeID: "n1", State: ranje.PsInactive}},
	}

	r6 := &ranje.Range{
		State:      ranje.RsSubsuming,
		Parents:    []ranje.Ident{2, 4},
		Children:   []ranje.Ident{7},
		Meta:       ranje.Meta{Ident: 6, End: ranje.Key("t")},
		Placements: []*ranje.Placement{{NodeID: "n2", State: ranje.PsInactive}},
	}

	r7 := &ranje.Range{
		State:      ranje.RsActive,
		Parents:    []ranje.Ident{5, 6},
		Children:   []ranje.Ident{},
		Meta:       ranje.Meta{Ident: 7},
		Placements: []*ranje.Placement{{NodeID: "n3", State: ranje.PsActive}},
	}

	input := []*ranje.Range{r1, r2, r3, r4, r5, r6, r7}

	pers := &FakePersister{ranges: input}
	ks, err := New(configForTest(), pers)
	require.NoError(t, err)

	// ----

	f, err := ks.Family(1)
	require.NoError(t, err)
	assert.Equal(t, r1, f.Range)
	assert.Empty(t, f.Parents)
	assert.Empty(t, f.Siblings)
	assert.Equal(t, []*ranje.Range{r2, r3}, f.Children)

	f, err = ks.Family(2)
	require.NoError(t, err)
	assert.Equal(t, r2, f.Range)
	assert.Equal(t, []*ranje.Range{r1}, f.Parents)
	assert.Equal(t, []*ranje.Range{r3}, f.Siblings)
	assert.Equal(t, []*ranje.Range{r6}, f.Children)

	f, err = ks.Family(3)
	require.NoError(t, err)
	assert.Equal(t, r3, f.Range)
	assert.Equal(t, []*ranje.Range{r1}, f.Parents)
	assert.Equal(t, []*ranje.Range{r2}, f.Siblings)
	assert.Equal(t, []*ranje.Range{r4, r5}, f.Children)

	f, err = ks.Family(4)
	require.NoError(t, err)
	assert.Equal(t, r4, f.Range)
	assert.Equal(t, []*ranje.Range{r3}, f.Parents)
	assert.Equal(t, []*ranje.Range{r5}, f.Siblings)
	assert.Equal(t, []*ranje.Range{r6}, f.Children)

	f, err = ks.Family(5)
	require.NoError(t, err)
	assert.Equal(t, r5, f.Range)
	assert.Equal(t, []*ranje.Range{r3}, f.Parents)
	assert.Equal(t, []*ranje.Range{r4}, f.Siblings)
	assert.Equal(t, []*ranje.Range{r7}, f.Children)

	f, err = ks.Family(6)
	require.NoError(t, err)
	assert.Equal(t, r6, f.Range)
	assert.Equal(t, []*ranje.Range{r2, r4}, f.Parents)
	assert.Empty(t, f.Siblings)
	assert.Equal(t, []*ranje.Range{r7}, f.Children)

	f, err = ks.Family(7)
	require.NoError(t, err)
	assert.Equal(t, r7, f.Range)
	assert.Equal(t, []*ranje.Range{r5, r6}, f.Parents)
	assert.Empty(t, f.Siblings)
	assert.Empty(t, f.Children)
}
