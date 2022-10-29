package keyspace

import (
	"fmt"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperations(t *testing.T) {

	//               ┌─────┐
	//         ┌─────│ 1 o │─────┐
	//         │     └─────┘     │
	//         ▼                 ▼
	//      ┌─────┐           ┌─────┐
	//    ┌─│ 2 o │─┐       ┌─│ 3 s │─┐
	//    │ └─────┘ │       │ └─────┘ │
	//    ▼         ▼       ▼         ▼
	// ┌─────┐   ┌─────┐ ┌─────┐   ┌─────┐
	// │ 4 j │   │ 5 j │ │ 6 a │   │ 7 a │
	// └─────┘   └─────┘ └─────┘   └─────┘
	//    │         │
	//    └────┬────┘
	//         ▼
	//      ┌─────┐
	//      │ 8 a │
	//      └─────┘
	//
	// R1 obsolete (was split into R2, R3 at ccc)
	// R2 obsolete (was split into R4, R5 at bbb)
	// R3 splitting into R6, R7 at ddd
	// R4 joining with R5 into R8
	// R5 joining with R4 into R8
	// R6 active (splitting from R3)
	// R7 active (splitting from R7)
	// R8 active (joining from R4, R5)

	r1 := &ranje.Range{
		State:      ranje.RsObsolete,
		Children:   []ranje.Ident{2, 3},
		Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey, End: ranje.ZeroKey},
		Placements: []*ranje.Placement{},
	}

	r2 := &ranje.Range{
		State:      ranje.RsObsolete,
		Parents:    []ranje.Ident{1},
		Children:   []ranje.Ident{4, 5},
		Meta:       ranje.Meta{Ident: 2, End: ranje.Key("ccc")},
		Placements: []*ranje.Placement{},
	}

	r3 := &ranje.Range{
		State:      ranje.RsSplitting,
		Parents:    []ranje.Ident{1},
		Children:   []ranje.Ident{6, 7},
		Meta:       ranje.Meta{Ident: 3, Start: ranje.Key("ccc")},
		Placements: []*ranje.Placement{},
	}

	r4 := &ranje.Range{
		State:      ranje.RsJoining,
		Parents:    []ranje.Ident{2},
		Children:   []ranje.Ident{8},
		Meta:       ranje.Meta{Ident: 4, End: ranje.Key("bbb")},
		Placements: []*ranje.Placement{},
	}

	r5 := &ranje.Range{
		State:      ranje.RsJoining,
		Parents:    []ranje.Ident{2},
		Children:   []ranje.Ident{8},
		Meta:       ranje.Meta{Ident: 5, Start: ranje.Key("bbb")},
		Placements: []*ranje.Placement{},
	}

	r6 := &ranje.Range{
		State:      ranje.RsActive,
		Parents:    []ranje.Ident{3},
		Children:   []ranje.Ident{},
		Meta:       ranje.Meta{Ident: 6, Start: ranje.Key("ccc"), End: ranje.Key("ddd")},
		Placements: []*ranje.Placement{},
	}

	r7 := &ranje.Range{
		State:      ranje.RsActive,
		Parents:    []ranje.Ident{3},
		Children:   []ranje.Ident{},
		Meta:       ranje.Meta{Ident: 7, Start: ranje.Key("ddd")},
		Placements: []*ranje.Placement{},
	}

	r8 := &ranje.Range{
		State:      ranje.RsActive,
		Parents:    []ranje.Ident{4, 5},
		Children:   []ranje.Ident{},
		Meta:       ranje.Meta{Ident: 8, End: ranje.Key("ccc")},
		Placements: []*ranje.Placement{},
	}

	pers := &FakePersister{
		ranges: []*ranje.Range{
			r1, r2, r3, r4, r5, r6, r7, r8}}

	ks, err := New(configForTest(), pers)
	require.NoError(t, err)

	ops, err := ks.Operations()
	require.NoError(t, err)
	require.Len(t, ops, 2)

	require.Contains(t, ops, &JoinOp{parents: []*ranje.Range{r4, r5}, child: r8})
	require.Contains(t, ops, &SplitOp{parent: r3, children: []*ranje.Range{r6, r7}})
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
					State:      ranje.RsActive,
					Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n1", State: ranje.PsInactive}},
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
					State:      ranje.RsJoining,
					Children:   []ranje.Ident{3},
					Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey, End: ranje.Key("ggg")},
					Placements: []*ranje.Placement{{NodeID: "n1", State: ranje.PsInactive}},
				},
				{
					State:      ranje.RsJoining,
					Children:   []ranje.Ident{3},
					Meta:       ranje.Meta{Ident: 2, Start: ranje.Key("ggg"), End: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n2", State: ranje.PsInactive}},
				},
				{
					State:      ranje.RsActive,
					Parents:    []ranje.Ident{1, 2},
					Meta:       ranje.Meta{Ident: 3, Start: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n3", State: ranje.PsInactive}},
				},
			},
			output: [][]string{
				{"children have not all given up"}, // n1, should not advance, because subsumed
				{"children have not all given up"}, // n2, same
				{""},                               // n3
			},
		},
	}

	for _, ex := range examples {
		pers := &FakePersister{ranges: ex.input}
		ks, err := New(configForTest(), pers)
		require.NoError(t, err)

		for r := 0; r < len(ex.input); r++ {
			for p := 0; p < len(ex.input[r].Placements); p++ {
				expected := ex.output[r][p] // error string; empty is nil
				actual := ks.PlacementMayActivate(ex.input[r].Placements[p])
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

func TestPlacementMayBeTaken(t *testing.T) {
	examples := []struct {
		name   string
		input  []*ranje.Range
		output [][]bool
	}{
		{
			name: "ready with no replacement",
			input: []*ranje.Range{
				{
					State:      ranje.RsActive,
					Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n1", State: ranje.PsActive}},
				},
			},
			output: [][]bool{
				{false}, // n1
			},
		},
		{
			name: "joining",
			input: []*ranje.Range{
				{
					State:      ranje.RsJoining,
					Children:   []ranje.Ident{3},
					Meta:       ranje.Meta{Ident: 1, Start: ranje.ZeroKey, End: ranje.Key("ggg")},
					Placements: []*ranje.Placement{{NodeID: "n1", State: ranje.PsActive}},
				},
				{
					State:      ranje.RsJoining,
					Children:   []ranje.Ident{3},
					Meta:       ranje.Meta{Ident: 2, Start: ranje.Key("ggg"), End: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n2", State: ranje.PsActive}},
				},
				{
					State:      ranje.RsActive,
					Parents:    []ranje.Ident{1, 2},
					Meta:       ranje.Meta{Ident: 3, Start: ranje.ZeroKey},
					Placements: []*ranje.Placement{{NodeID: "n3", State: ranje.PsInactive}},
				},
			},
			output: [][]bool{
				{true},  // n1
				{true},  // n2
				{false}, // n3
			},
		},
	}

	for _, ex := range examples {
		pers := &FakePersister{ranges: ex.input}
		ks, err := New(configForTest(), pers)
		require.NoError(t, err)

		for r := 0; r < len(ex.input); r++ {
			for p := 0; p < len(ex.input[r].Placements); p++ {
				assert.Equal(t, ex.output[r][p], ks.PlacementMayDeactivate(ex.input[r].Placements[p]))
			}
		}
	}
}

// -----------------------------------------------------------------------------

// TODO: Remove this when global config goes away.
func configForTest() config.Config {
	return config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
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
