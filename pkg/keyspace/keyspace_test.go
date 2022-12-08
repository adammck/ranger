package keyspace

import (
	"fmt"
	"testing"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperations(t *testing.T) {
	ks := testFixtureR3(t)
	r := rangeGetter(t, ks)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Len(t, ops, 2)

	require.Equal(t, ops[0], NewOperation([]*ranje.Range{r(3)}, []*ranje.Range{r(6), r(7)}))
	for rID, b := range map[api.RangeID]bool{1: false, 2: false, 3: true, 4: false, 5: false, 6: false, 7: false, 8: false} {
		require.Equal(t, b, ops[0].isDirection(Source, rID), "i=0, rID=%v", rID)
	}
	for rID, b := range map[api.RangeID]bool{1: false, 2: false, 3: false, 4: false, 5: false, 6: true, 7: true, 8: false} {
		require.Equal(t, b, ops[0].isDirection(Dest, rID), "i=0, rID=%v", rID)
	}

	require.Equal(t, ops[1], NewOperation([]*ranje.Range{r(4), r(5)}, []*ranje.Range{r(8)}))
	for rID, b := range map[api.RangeID]bool{1: false, 2: false, 3: false, 4: true, 5: true, 6: false, 7: false, 8: false} {
		require.Equal(t, b, ops[1].isDirection(Source, rID), "i=0, rID=%v", rID)
	}
	for rID, b := range map[api.RangeID]bool{1: false, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false, 8: true} {
		require.Equal(t, b, ops[1].isDirection(Dest, rID), "i=0, rID=%v", rID)
	}
}

func TestNoOperations(t *testing.T) {
	r1 := &ranje.Range{
		State:    api.RsActive,
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 1, Start: api.ZeroKey, End: api.ZeroKey},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1}}

	ks, err := New(pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Empty(t, ops)
}

func TestSplitIntoThree(t *testing.T) {

	//          ┌─────┐
	//    ┌─────│ 1 s │─────┐
	//    │     └─────┘     │
	//    │        │        │
	//    ▼        ▼        ▼
	// ┌─────┐  ┌─────┐  ┌─────┐
	// │ 2 a │  │ 3 a │  │ 4 a │
	// └─────┘  └─────┘  └─────┘
	//
	// Hypothetical split into three child ranges. The keyspace doesn't prevent
	// this (currently), though doesn't provide any methods to make it so. It's
	// supported by default by the operations interface, just because it would
	// be more work to prevent it.

	r1 := &ranje.Range{
		State:    api.RsSubsuming,
		Children: []api.RangeID{2, 3, 4},
		Meta:     api.Meta{Ident: 1},
	}

	r2 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 2, End: api.Key("bbb")},
	}

	r3 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 3, Start: api.Key("bbb"), End: api.Key("ccc")},
	}

	r4 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 4, Start: api.Key("ccc")},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1, r2, r3, r4}}

	ks, err := New(pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Contains(t, ops, NewOperation([]*ranje.Range{r1}, []*ranje.Range{r2, r3, r4}))
	require.Len(t, ops, 1)
}

func TestJoinFromThree(t *testing.T) {

	//          ┌─────┐
	//    ┌─────│ 1 o │─────┐
	//    │     └─────┘     │
	//    │        │        │
	//    ▼        ▼        ▼
	// ┌─────┐  ┌─────┐  ┌─────┐
	// │ 2 j │  │ 3 j │  │ 4 j │
	// └─────┘  └─────┘  └─────┘
	//    │        │        │
	//    │        ▼        │
	//    │     ┌─────┐     │
	//    └────▶│ 5 a │◀────┘
	//          └─────┘
	//
	// Hypothetical join from three ranges into one.

	r1 := &ranje.Range{
		State:    api.RsObsolete,
		Children: []api.RangeID{2, 3, 4},
		Meta:     api.Meta{Ident: 1},
	}

	r2 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{5},
		Meta:     api.Meta{Ident: 2, End: api.Key("bbb")},
	}

	r3 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{5},
		Meta:     api.Meta{Ident: 3, Start: api.Key("bbb"), End: api.Key("ccc")},
	}

	r4 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{5},
		Meta:     api.Meta{Ident: 4, Start: api.Key("ccc")},
	}

	r5 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{2, 3, 4},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 5},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1, r2, r3, r4, r5}}

	ks, err := New(pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Contains(t, ops, NewOperation([]*ranje.Range{r2, r3, r4}, []*ranje.Range{r5}))
	require.Len(t, ops, 1)
}

func TestSplitIntoOne(t *testing.T) {
	t.Skip("not supported for now")

	// ┌─────┐
	// │ 1 s │
	// └─────┘
	//    │
	//    ▼
	// ┌─────┐
	// │ 2 a │
	// └─────┘
	//
	// Another hypothetical split of one into... one? This should definitely not
	// happen, but it's not the concern of the Operation interface, so make sure
	// we don't do anything too weird with it.

	r1 := &ranje.Range{
		State:    api.RsSubsuming,
		Children: []api.RangeID{2},
		Meta:     api.Meta{Ident: 1},
	}

	r2 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 2},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1, r2}}

	ks, err := New(pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Contains(t, ops, NewOperation([]*ranje.Range{r1}, []*ranje.Range{r2}))
	require.Len(t, ops, 1)
}

func TestJoinFromOne(t *testing.T) {
	t.Skip("not supported for now")

	// ┌─────┐
	// │ 1 j │
	// └─────┘
	//    │
	//    ▼
	// ┌─────┐
	// │ 2 a │
	// └─────┘
	//
	// Sure, why not.

	r1 := &ranje.Range{
		State:    api.RsSubsuming,
		Children: []api.RangeID{2},
		Meta:     api.Meta{Ident: 1},
	}

	r2 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 2},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1, r2}}

	ks, err := New(pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Contains(t, ops, NewOperation([]*ranje.Range{r1}, []*ranje.Range{r2}))
	require.Len(t, ops, 1)
}

func getOneOperation(t *testing.T, ks *Keyspace, i int) (*Operation, error) {
	ops, err := ks.Operations()
	if err != nil {
		return nil, err
	}

	switch len(ops) {
	case 0:
		return nil, nil
	case 1:
		return ops[0], nil
	}

	return nil, fmt.Errorf("bug in test: only supports zero or one ops")
}

func TestPlacementMayBecomeReady(t *testing.T) {
	examples := []struct {
		name   string
		input  []*ranje.Range
		output [][]string // error string; empty is nil
	}{
		{
			name: "initial",
			input: []*ranje.Range{
				{
					State:      api.RsActive,
					Meta:       api.Meta{Ident: 1, Start: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n1", StateCurrent: api.PsInactive}},
				},
			},
			output: [][]string{
				{""}, // n1
			},
		},
		{
			name: "joining",
			input: []*ranje.Range{
				{
					State:      api.RsSubsuming,
					Children:   []api.RangeID{3},
					Meta:       api.Meta{Ident: 1, Start: api.ZeroKey, End: api.Key("ggg")},
					Placements: []*ranje.Placement{{NodeID: "n1", StateCurrent: api.PsInactive}},
				},
				{
					State:      api.RsSubsuming,
					Children:   []api.RangeID{3},
					Meta:       api.Meta{Ident: 2, Start: api.Key("ggg"), End: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n2", StateCurrent: api.PsInactive}},
				},
				{
					State:      api.RsActive,
					Parents:    []api.RangeID{1, 2},
					Meta:       api.Meta{Ident: 3, Start: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n3", StateCurrent: api.PsInactive}},
				},
			},
			output: [][]string{
				{"never activate backside"}, // n1, should not advance, because subsumed
				{"never activate backside"}, // n2, same
				{""},                        // n3
			},
		},
	}

	for i, ex := range examples {
		pers := &FakePersister{ranges: ex.input}
		ks, err := New(pers)
		require.NoError(t, err, "i=%d", i)

		op, err := getOneOperation(t, ks, i)
		require.NoError(t, err, "i=%d", i)

		for r := 0; r < len(ex.input); r++ {
			for p := 0; p < len(ex.input[r].Placements); p++ {
				expected := ex.output[r][p] // error string; empty is nil
				actual := op.MayActivate(ex.input[r].Placements[p], ex.input[r])
				msg := fmt.Sprintf("example=%s, range=%d, placement=%d", ex.name, r, p)

				if expected == "" {
					assert.NoError(t, actual, msg)
					continue
				}

				if assert.Error(t, actual, msg) {
					assert.Equal(t, expected, actual.Error(), msg)
				}
			}
		}
	}
}

func TestPlacementMayBeDeactivated(t *testing.T) {
	examples := []struct {
		name   string
		input  []*ranje.Range
		output [][]string
	}{
		{
			name: "active with no replacement",
			input: []*ranje.Range{
				{
					State:      api.RsActive,
					Meta:       api.Meta{Ident: 1, Start: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n1", StateCurrent: api.PsActive}},
				},
			},
			output: [][]string{
				{"no reason"}, // n1
			},
		},
		{
			name: "joining",
			input: []*ranje.Range{
				{
					State:      api.RsSubsuming,
					Children:   []api.RangeID{3},
					Meta:       api.Meta{Ident: 1, Start: api.ZeroKey, End: api.Key("ggg")},
					Placements: []*ranje.Placement{{NodeID: "n1", StateCurrent: api.PsActive}},
				},
				{
					State:      api.RsSubsuming,
					Children:   []api.RangeID{3},
					Meta:       api.Meta{Ident: 2, Start: api.Key("ggg"), End: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n2", StateCurrent: api.PsActive}},
				},
				{
					State:      api.RsActive,
					Parents:    []api.RangeID{1, 2},
					Meta:       api.Meta{Ident: 3, Start: api.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n3", StateCurrent: api.PsInactive}},
				},
			},
			output: [][]string{
				{""},                             // n1
				{""},                             // n2
				{"placment not in api.PsActive"}, // n3
			},
		},
	}

	for i, ex := range examples {
		pers := &FakePersister{ranges: ex.input}
		ks, err := New(pers)
		require.NoError(t, err, "i=%d", i)

		op, err := getOneOperation(t, ks, i)
		require.NoError(t, err, "i=%d", i)

		for r := 0; r < len(ex.input); r++ {
			for p := 0; p < len(ex.input[r].Placements); p++ {
				expected := ex.output[r][p] // error string; empty is nil
				actual := op.MayDeactivate(ex.input[r].Placements[p], ex.input[r])
				msg := fmt.Sprintf("example=%s, range=%d, placement=%d", ex.name, r, p)

				if expected == "" {
					assert.NoError(t, actual, msg)
					continue
				}

				if assert.Error(t, actual, msg) {
					assert.Equal(t, expected, actual.Error(), msg)
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------

// TODO: Unify this with the one in orchestrator_test.go
type FakePersister struct {
	ranges []*ranje.Range
}

func (fp *FakePersister) GetRanges() ([]*ranje.Range, error) {
	return fp.ranges, nil
}

func (fp *FakePersister) PutRanges([]*ranje.Range) error {
	return nil
}
