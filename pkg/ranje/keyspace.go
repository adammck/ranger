package ranje

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
)

// Keyspace is an overlapping set of ranges which cover all of the possible
// values in the space. Any value is covered by either one or two ranges: one
// range in the steady-state, where nothing is being moved around, and two
// ranges while rebalancing is in progress.
//
// TODO: Move this out of 'ranje' package; it's stateful.
type Keyspace struct {
	pers     Persister
	ranges   []*Range // TODO: don't be dumb, use an interval tree
	mu       sync.RWMutex
	maxIdent uint64
}

type RangeGetter interface {
	Get(id Ident) (*Range, error)
}

func New(persister Persister) *Keyspace {
	ks := &Keyspace{
		pers: persister,
	}

	ranges, err := persister.GetRanges()
	if err != nil {
		panic(fmt.Sprintf("error from GetRanges: %v", err))
	}

	log.Printf("got %d ranges from store\n", len(ranges))

	// Special case: There are no ranges in the store. We are bootstrapping the
	// keyspace from scratch, so start with a singe range that covers all keys.
	if len(ranges) == 0 {
		r := ks.Range()
		r.State = Pending
		ks.ranges = []*Range{r}
		ks.pers.PutRanges(ks.ranges)
		return ks
	}

	ks.ranges = ranges
	for _, r := range ks.ranges {

		// Repair the range.
		r.pers = persister

		// Repair the placements
		for _, p := range []*Placement{r.CurrentPlacement, r.NextPlacement} {
			if p != nil {
				p.rang = r
			}
		}

		// Repair the maxIdent cache.
		if r.Meta.Ident.Key > ks.maxIdent {
			ks.maxIdent = r.Meta.Ident.Key
		}
	}

	return ks
}

// DangerousDebuggingMethods returns a keyspaceDebug. Handle with care!
func (ks *Keyspace) DangerousDebuggingMethods() *keyspaceDebug {
	return &keyspaceDebug{ks}
}

// NewWithSplits is just for testing.
// TODO: Move this to the tests, why is it here?
func NewWithSplits(persister Persister, splits []string) *Keyspace {
	ks := &Keyspace{
		pers: persister,
	}
	rs := make([]*Range, len(splits)+1)

	// TODO: Should we sort the splits here? Or panic? We currently assume they're sorted.

	for i := range rs {
		var s, e Key

		if i > 0 {
			s = rs[i-1].Meta.End
		} else {
			s = ZeroKey
		}

		if i < len(splits) {
			e = Key(splits[i])
		} else {
			e = ZeroKey
		}

		// TODO: Move start/end into params of Range? No sense without them.
		// Note: Not persisting the new range, because tests.
		r := ks.Range()
		r.Meta.Start = s
		r.Meta.End = e
		rs[i] = r
	}

	ks.ranges = rs
	return ks
}

func (ks *Keyspace) LogRanges() {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	var sb strings.Builder
	for i, r := range ks.ranges {
		sb.WriteString(r.LogString())

		if i < len(ks.ranges)-1 {
			sb.WriteString(", ")
		}
	}

	log.Printf("Keyspace: {%s}", sb.String())
}

// Range returns a new range with the next available ident. This is the only
// way that a Range should be constructed. Callers must call Range.InitPersist
// on the resulting range, maybe after mutating it once.
func (ks *Keyspace) Range() *Range {
	ks.maxIdent += 1

	r := &Range{
		pers:  ks.pers,
		State: Pending,
		Meta: Meta{
			Ident: Ident{
				Scope: "", // ???
				Key:   ks.maxIdent,
			},
		},
		// Starts dirty, because it hasn't been persisted yet.
		dirty: true,
	}

	return r
}

func (ks *Keyspace) RangesByState(s StateLocal) []*Range {
	out := []*Range{}

	for _, r := range ks.ranges {
		if r.State == s {
			out = append(out, r)
		}
	}

	return out
}

type PBNID struct {
	Range     *Range
	Placement *Placement
	Position  uint8
}

// NodeChecker only exists to pass a Roster to RangesOnNonExistentNodes without
// importing the whole thing.
type NodeChecker interface {
	NodeExists(nodeIdent string) bool
}

func (ks *Keyspace) RangesOnNonExistentNodes(nc NodeChecker) []PBNID {
	out := []PBNID{}

	for _, r := range ks.ranges {
		for i, p := range [2]*Placement{r.CurrentPlacement, r.NextPlacement} {
			if p != nil && !nc.NodeExists(p.NodeID) {
				out = append(out, PBNID{r, p, uint8(i)})
			}
		}
	}

	return out
}

// PlacementsByNodeID returns a list of (range, placement, position) tuples for
// the given nodeID.
//
// This is intended for debugging. If you are using this during rebalancing,
// you're probably doing something very wrong. It's currently extremely slow.
//
// Note that the placements are pointers, so may mutate after returning! Don't
// fuck around with them.
func (ks *Keyspace) PlacementsByNodeID(nID string) []PBNID {
	out := []PBNID{}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	// TODO: Wow this is dumb! Keep an index of this somewhere.
	for _, r := range ks.ranges {
		for i, p := range [2]*Placement{r.CurrentPlacement, r.NextPlacement} {
			if p != nil {
				if p.NodeID == nID {
					out = append(out, PBNID{r, p, uint8(i)})
				}
			}
		}
	}

	return out
}

func (ks *Keyspace) LogString() string {
	s := make([]string, len(ks.ranges))

	for i, r := range ks.ranges {
		s[i] = r.LogString()
	}

	return strings.Join(s, " ")
}

// Get returns a range by its ident, or an error if no such range exists.
// TODO: Allow getting by other things.
// TODO: Should this lock ranges? Or the caller do it?
func (ks *Keyspace) Get(id Ident) (*Range, error) {
	for _, r := range ks.ranges {
		if r.Meta.Ident == id {
			return r, nil
		}
	}

	return nil, fmt.Errorf("no such range: %s", id.String())
}

// Len returns the number of ranges.
// This is mostly for testing, maybe remove it.
func (ks *Keyspace) Len() int {
	return len(ks.ranges)
}

// RangeToState tries to move the given range into the given state.
// TODO: Can we drop this and let range state transitions happen via Placement?
func (ks *Keyspace) RangeToState(rang *Range, state StateLocal) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	err := rang.toState(state, ks)
	if err != nil {
		return err
	}

	return ks.mustPersistDirtyRanges()
}

func (ks *Keyspace) PlacementToState(p *Placement, state StatePlacement) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	err := p.toState(state)
	if err != nil {
		return err
	}

	p.rang.placementStateChanged(ks)
	return ks.mustPersistDirtyRanges()
}

func (ks *Keyspace) mustPersistDirtyRanges() error {
	ranges := []*Range{}
	for _, r := range ks.ranges {
		if r.dirty {
			ranges = append(ranges, r)
		}
	}

	err := ks.pers.PutRanges(ranges)
	if err != nil {
		panic(fmt.Sprintf("failed to persist ranges: %v", err))
		//return err
	}

	return nil
}

// TODO: Rename to Split once the old one is gone
func (ks *Keyspace) DoSplit(r *Range, k Key) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if k == ZeroKey {
		return fmt.Errorf("can't split on zero key")
	}

	if r.State != Ready {
		return errors.New("can't split non-ready range")
	}

	// This should not be possible. Panic?
	if len(r.Children) > 0 {
		return fmt.Errorf("range %s already has %d children", r, len(r.Children))
	}

	if !r.Meta.Contains(k) {
		return fmt.Errorf("range %s does not contain key: %s", r, k)
	}

	if k == r.Meta.Start {
		return fmt.Errorf("range %s starts with key: %s", r, k)
	}

	// Change the state of the splitting range directly via Range.toState rather
	// than Keyspace.ToState as (usually!) recommended, because we don't want
	// to persist the change until the two new ranges have been created, below.
	err := r.toState(Splitting, ks)
	if err != nil {
		// The error is clear enough, no need to wrap it.
		return err
	}

	one := ks.Range()
	one.Meta.Start = r.Meta.Start
	one.Meta.End = k
	one.Parents = []Ident{r.Meta.Ident}

	two := ks.Range()
	two.Meta.Start = k
	two.Meta.End = r.Meta.End
	two.Parents = []Ident{r.Meta.Ident}

	// append to the end of the ranges
	// TODO: Insert the children after the parent, not at the end!
	ks.ranges = append(ks.ranges, one)
	ks.ranges = append(ks.ranges, two)

	r.Children = []Ident{
		one.Meta.Ident,
		two.Meta.Ident,
	}

	// Persist all three ranges.
	ks.mustPersistDirtyRanges()

	return nil
}

func (ks *Keyspace) JoinTwo(one *Range, two *Range) (*Range, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	for _, r := range []*Range{one, two} {
		if r.State != Ready {
			return nil, errors.New("can't join non-ready ranges")
		}

		// This should not be possible. Panic?
		if len(r.Children) > 0 {
			return nil, fmt.Errorf("range %s already has %d children", r, len(r.Children))
		}
	}

	if one.Meta.End != two.Meta.Start {
		return nil, fmt.Errorf("not adjacent: %s, %s", one, two)
	}

	for _, r := range []*Range{one, two} {
		err := r.toState(Joining, ks)
		if err != nil {
			// The error is clear enough, no need to wrap it.
			return nil, err
		}
	}

	three := ks.Range()
	three.Meta.Start = one.Meta.Start
	three.Meta.End = two.Meta.End
	three.Parents = []Ident{one.Meta.Ident, two.Meta.Ident}

	// Insert new range at the end.
	ks.ranges = append(ks.ranges, three)

	one.Children = []Ident{three.Meta.Ident}
	two.Children = []Ident{three.Meta.Ident}

	// Persist all three ranges atomically.
	ks.mustPersistDirtyRanges()

	return three, nil
}

// index returns the index (in ks.ranges) of the given range.
// This should only be called while mu is held.
func (ks *Keyspace) index(r *Range) (int, error) {
	for i, rr := range ks.ranges {
		if r == rr {
			return i, nil
		}
	}

	return 0, fmt.Errorf("range %s not in keyspace", r)
}

// Discard removes a range from the keyspace. This is only allowed if the full
// range is covered by other ranges. As such this is called after a Split or a
// Merge to clean up.
func (ks *Keyspace) Discard(r *Range) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	log.Printf("discarding: %s", r.String())

	if r.State != Obsolete {
		return errors.New("can't discard non-obsolete range")
	}

	// TODO: Is this necessary? Ranges are generally discarded after split/join, but so what?
	if len(r.Children) == 0 {
		return fmt.Errorf("range %s has no children", r)
	}

	i, err := ks.index(r)
	if err != nil {
		return err
	}

	// remove the range
	// https://github.com/golang/go/wiki/SliceTricks
	copy(ks.ranges[i:], ks.ranges[i+1:])
	ks.ranges[len(ks.ranges)-1] = nil
	ks.ranges = ks.ranges[:len(ks.ranges)-1]

	return nil
}
