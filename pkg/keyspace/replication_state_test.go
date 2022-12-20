package keyspace

import (
	"testing"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"gotest.tools/assert"
)

func TestFlatRanges_Empty(t *testing.T) {
	// ┌─────┐
	// │ 1 a │
	// └─────┘
	//
	// Just one range covering the whole keyspace.

	r1 := &ranje.Range{
		State: api.RsActive,
		Meta:  api.Meta{Ident: 1},
	}

	ks := makeKeyspace(t, r1)

	actual := flatRanges(ks)
	assert.DeepEqual(t, []Repl{
		{
			Start: api.ZeroKey,
			End:   api.ZeroKey,
		},
	}, actual)
}

func TestFlatRanges_ThreeStable(t *testing.T) {
	//          ┌─────┐
	//    ┌─────│ 1 o │─────┐
	//    │     └─────┘     │
	//    │        │        │
	//    ▼        ▼        ▼
	// ┌─────┐  ┌─────┐  ┌─────┐
	// │ 2 a │  │ 3 a │  │ 4 a │
	// └─────┘  └─────┘  └─────┘
	//
	// Three active ranges which were split out from the genesis range, which is
	// now obsolete. Should return flat ranges with the same boundaries.

	r1 := &ranje.Range{
		State:    api.RsObsolete,
		Children: []api.RangeID{2, 3, 4},
		Meta: api.Meta{
			Ident: 1,
			Start: api.ZeroKey,
			End:   api.ZeroKey,
		},
	}

	r2 := &ranje.Range{
		State:   api.RsActive,
		Parents: []api.RangeID{1},
		Meta: api.Meta{
			Ident: 2,
			Start: api.ZeroKey,
			End:   api.Key("nnn"),
		},
	}

	r3 := &ranje.Range{
		State:   api.RsActive,
		Parents: []api.RangeID{1},
		Meta: api.Meta{
			Ident: 3,
			Start: api.Key("nnn"),
			End:   api.Key("ttt"),
		},
	}

	r4 := &ranje.Range{
		State:   api.RsActive,
		Parents: []api.RangeID{1},
		Meta: api.Meta{
			Ident: 4,
			Start: api.Key("ttt"),
			End:   api.ZeroKey,
		},
	}

	ks := makeKeyspace(t, r1, r2, r3, r4)

	actual := flatRanges(ks)
	assert.DeepEqual(t, []Repl{
		{
			Start: api.ZeroKey,
			End:   api.Key("nnn"),
		},
		{
			Start: api.Key("nnn"),
			End:   api.Key("ttt"),
		},
		{
			Start: api.Key("ttt"),
			End:   api.ZeroKey,
		},
	}, actual)
}

func TestFlatRanges_FromFixture(t *testing.T) {
	ks := testFixtureR3(t)

	actual := ks.ReplicationState()
	assert.DeepEqual(t, []Repl{
		{
			Start:  api.ZeroKey,
			End:    api.Key("bbb"),
			Total:  6,
			Active: 3,
		},
		{
			Start:  api.Key("bbb"),
			End:    api.Key("ccc"),
			Total:  6,
			Active: 2,
		},
		{
			Start:  api.Key("ccc"),
			End:    api.Key("ddd"),
			Total:  6,
			Active: 3,
		},
		{
			Start:  api.Key("ddd"),
			End:    api.ZeroKey,
			Total:  6,
			Active: 2,
		},
	}, actual)
}

func TestReplicationState_OneRange(t *testing.T) {
	//
	// ┌─────┐
	// │ 1 a │
	// | aii |
	// └─────┘
	//
	// Just one range covering the whole keyspace, but with three placements:
	// one active, two inactive.

	r1 := &ranje.Range{
		State: api.RsActive,
		Meta:  api.Meta{Ident: 1},
		Placements: []*ranje.Placement{
			{
				NodeID:       "aaa",
				StateCurrent: api.PsActive,
				StateDesired: api.PsActive,
			},
			{
				NodeID:       "bbb",
				StateCurrent: api.PsInactive,
				StateDesired: api.PsInactive,
			},
			{
				NodeID:       "bbb",
				StateCurrent: api.PsInactive,
				StateDesired: api.PsInactive,
			},
		},
	}

	ks := makeKeyspace(t, r1)

	actual := ks.ReplicationState()
	assert.DeepEqual(t, []Repl{
		{
			Start:  api.ZeroKey,
			End:    api.ZeroKey,
			Total:  3,
			Active: 1,
		},
	}, actual)
}

func TestReplicationState_FromFixture(t *testing.T) {
	ks := testFixtureR3(t)

	actual := ks.ReplicationState()
	assert.DeepEqual(t, []Repl{
		{
			Start:  api.ZeroKey,
			End:    "bbb",
			Total:  6,
			Active: 3,
		},
		{
			Start:  "bbb",
			End:    "ccc",
			Total:  6,
			Active: 2,
		},
		{
			Start:  "ccc",
			End:    "ddd",
			Total:  6,
			Active: 3,
		},
		{
			Start:  "ddd",
			End:    api.ZeroKey,
			Total:  6,
			Active: 2,
		},
	}, actual)
}
