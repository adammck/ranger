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
