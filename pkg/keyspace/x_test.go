package keyspace

import (
	"testing"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
)

func testFixtureR3(t *testing.T) *Keyspace {
	p := true

	//               ┌─────┐
	//         ┌─────│ 1 o │─────┐
	//         │     | ... |     │
	//         │     └─────┘     │
	//         ▼                 ▼
	//      ┌─────┐           ┌─────┐
	//    ┌─│ 2 o │─┐       ┌─│ 3 s │─┐
	//    │ | ... | │       │ | iia | │
	//    │ └─────┘ │       │ └─────┘ │
	//    ▼         ▼       ▼         ▼
	// ┌─────┐   ┌─────┐ ┌─────┐   ┌─────┐
	// │ 4 j │   │ 5 j │ │ 6 a │   │ 7 a │
	// │ iaa │   │ iia │ │ aai │   │ aii │
	// └─────┘   └─────┘ └─────┘   └─────┘
	//    │         │
	//    └────┬────┘
	//         ▼
	//      ┌─────┐
	//      │ 8 a │
	//      │ aii │
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
		State:      api.RsObsolete,
		Children:   []api.RangeID{2, 3},
		Meta:       api.Meta{Ident: 1, Start: api.ZeroKey, End: api.ZeroKey},
		Placements: []*ranje.Placement{},
	}

	r2 := &ranje.Range{
		State:      api.RsObsolete,
		Parents:    []api.RangeID{1},
		Children:   []api.RangeID{4, 5},
		Meta:       api.Meta{Ident: 2, End: api.Key("ccc")},
		Placements: []*ranje.Placement{},
	}

	r3 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{6, 7},
		Meta:     api.Meta{Ident: 3, Start: api.Key("ccc")},
	}
	if p {
		r3.Placements = []*ranje.Placement{
			{NodeID: "aaa", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "bbb", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "ccc", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
		}
	}

	r4 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{2},
		Children: []api.RangeID{8},
		Meta:     api.Meta{Ident: 4, End: api.Key("bbb")},
	}
	if p {
		r4.Placements = []*ranje.Placement{
			{NodeID: "ddd", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "eee", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "fff", StateCurrent: api.PsActive, StateDesired: api.PsActive},
		}
	}

	r5 := &ranje.Range{
		State:    api.RsSubsuming,
		Parents:  []api.RangeID{2},
		Children: []api.RangeID{8},
		Meta:     api.Meta{Ident: 5, Start: api.Key("bbb"), End: api.Key("ccc")},
	}
	if p {
		r5.Placements = []*ranje.Placement{
			{NodeID: "ggg", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "hhh", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "iii", StateCurrent: api.PsActive, StateDesired: api.PsActive},
		}
	}

	r6 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{3},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 6, Start: api.Key("ccc"), End: api.Key("ddd")},
	}
	if p {
		r6.Placements = []*ranje.Placement{
			{NodeID: "jjj", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "kkk", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "lll", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
		}
	}

	r7 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{3},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 7, Start: api.Key("ddd")},
	}
	if p {
		r7.Placements = []*ranje.Placement{
			{NodeID: "mmm", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "nnn", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "ooo", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
		}
	}

	r8 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{4, 5},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 8, End: api.Key("ccc")},
	}
	if p {
		r8.Placements = []*ranje.Placement{
			{NodeID: "mmm", StateCurrent: api.PsActive, StateDesired: api.PsActive},
			{NodeID: "nnn", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
			{NodeID: "ooo", StateCurrent: api.PsInactive, StateDesired: api.PsInactive},
		}
	}

	return makeKeyspace(t, r1, r2, r3, r4, r5, r6, r7, r8)
}

func makeKeyspace(t *testing.T, ranges ...*ranje.Range) *Keyspace {
	pers := &FakePersister{ranges: ranges}
	ks, err := New(pers, ranje.R1)
	if err != nil {
		t.Fatalf("unexpected failure making keyspace: %v", err)
		return nil // unreachable
	}

	return ks
}

// mustGetRange returns a range from the given keyspace or fails the test.
// TODO: This is pasted from orchestrator_test; deduplicate it.
func mustGetRange(t *testing.T, ks *Keyspace, rID int) *ranje.Range {
	r, err := ks.GetRange(api.RangeID(rID))
	if err != nil {
		t.Fatalf("ks.GetRange(%d): %v", rID, err)
		return nil // unreachable
	}
	return r
}

// rangeGetter partially applies mustGetRange.
func rangeGetter(t *testing.T, ks *Keyspace) func(rID int) *ranje.Range {
	return func(rID int) *ranje.Range {
		return mustGetRange(t, ks, rID)
	}
}
