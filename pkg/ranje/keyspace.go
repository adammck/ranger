package ranje

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/config"
)

// Keyspace is an overlapping set of ranges which cover all of the possible
// values in the space. Any value is covered by either one or two ranges: one
// range in the steady-state, where nothing is being moved around, and two
// ranges while rebalancing is in progress.
//
// TODO: Move this out of 'ranje' package; it's stateful.
type Keyspace struct {
	cfg      config.Config
	pers     Persister
	ranges   []*Range // TODO: don't be dumb, use an interval tree
	mu       sync.RWMutex
	maxIdent Ident
}

type RangeGetter interface {
	Get(id Ident) (*Range, error)
}

func New(cfg config.Config, persister Persister) *Keyspace {
	ks := &Keyspace{
		cfg:  cfg,
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
		ks.ranges = []*Range{r}
		ks.pers.PutRanges(ks.ranges)
		return ks
	}

	ks.ranges = ranges
	for _, r := range ks.ranges {

		// Repair the range.
		r.pers = persister

		// Repair the placements
		for _, p := range r.Placements {
			if p != nil {
				p.rang = r
			}
		}

		// Repair the maxIdent cache.
		if r.Meta.Ident > ks.maxIdent {
			ks.maxIdent = r.Meta.Ident
		}
	}

	// Sanity-check the ranges for no gaps and no overlaps.
	err = ks.SanityCheck()
	if err != nil {
		// TODO: Not this
		panic(fmt.Sprintf("failed sanity check: %v", err))
	}

	return ks
}

// SanityCheck returns an error if the curernt range state isn't sane. It does
// nothing to try to rectify the situaton. This method mostly compensates for
// the fact that I'm storing all these ranges in a single flat slice.
func (ks *Keyspace) SanityCheck() error {

	// Check for no duplicate range IDs.

	seen := map[Ident]struct{}{}
	for _, r := range ks.ranges {
		if _, ok := seen[r.Meta.Ident]; ok {
			return fmt.Errorf("duplicate range ID (i=%d)", r.Meta.Ident)
		}
		seen[r.Meta.Ident] = struct{}{}
	}

	// Check that leaf ranges (i.e. those with no children) cover the entire
	// keyspace with no overlaps. This must always be the case; we only persist
	// valid configurations, and do it transactionally.

	leafs := []*Range{}
	for i := range ks.ranges {
		if len(ks.ranges[i].Children) == 0 {
			leafs = append(leafs, ks.ranges[i])
		}
	}

	sort.Slice(leafs, func(i, j int) bool {
		m1 := leafs[i].Meta
		m2 := leafs[j].Meta

		if m1.Start != m2.Start {
			return m1.Start < m2.Start
		}

		if m1.End != m2.End {
			return m1.End < m2.End
		}

		// We know there's no duplicate Idents already.
		return m1.Ident < m2.Ident
	})

	for i, r := range leafs {
		if r.State == RsSubsuming || r.State == RsObsolete {
			return fmt.Errorf("non-active leaf range with no children (rID=%v)", r.Meta.Ident)
		}

		if i == 0 { // first range
			if r.Meta.Start != ZeroKey {
				return fmt.Errorf("first leaf range did not start with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not first range
			if r.Meta.Start == ZeroKey {
				return fmt.Errorf("non-first leaf range started with zero key (rID=%d)", r.Meta.Ident)
			}
			if r.Meta.Start != leafs[i-1].Meta.End {
				return fmt.Errorf("leaf range does not begin at prior leaf range end (i=%d)", i)
			}
		}
		if i == len(leafs)-1 { // last range
			if r.Meta.End != ZeroKey {
				return fmt.Errorf("last leaf range did not end with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not last range
			if r.Meta.End == ZeroKey {
				return fmt.Errorf("non-last leaf range ended with zero key (rID=%d)", r.Meta.Ident)
			}
		}
	}

	return nil
}

func (ks *Keyspace) Ranges() ([]*Range, func()) {
	ks.mu.Lock()
	return ks.ranges, ks.mu.Unlock
}

// PlacementMayBecomeReady returns whether the given placement is permitted to
// advance from PsPrepared to PsReady.
//
// Returns error if called when the placement is in any state other than
// PsPrepared.
func (ks *Keyspace) PlacementMayBecomeReady(p *Placement) bool {

	// Sanity check.
	if p.State != PsPrepared {
		log.Printf("called PlacementMayBecomeReady in weird state: %s", p.State)
		return false
	}

	r := p.Range()

	// Gather the parent ranges
	// TODO: Move this to a method on Keyspace
	parents := make([]*Range, len(r.Parents))
	for i, rID := range r.Parents {
		rp, err := ks.Get(rID)
		if err != nil {
			log.Printf("range has invalid parent: %s", rID)
			return false
		}

		parents[i] = rp
	}

	// If this placement is part of a child range, we must wait until all of the
	// placements in the parent range have been taken. Otherwise, keys will be
	// available in both the parent and child.
	for _, rp := range parents {
		for _, pp := range rp.Placements {
			if pp.State == PsReady {
				return false
			}
		}
	}

	n := 0
	for _, pp := range r.Placements {
		if pp.State == PsReady {
			n += 1
		}
	}

	return n < r.MinReady()
}

// MayBeTaken returns whether the given placement, which is assumed to be in
// state PsReady, may advance to PsTaken. The only circumstance where this is
// true is when the placement is being replaced by another replica (i.e. a move)
// or when the entire range has been subsumed.
//
// TODO: Implement the latter, when (re-)implementing splits and joins. This
//       probably needs to move to the keyspace, for that.
//
func (ks *Keyspace) PlacementMayBeTaken(p *Placement) bool {

	// Sanity check.
	if p.State != PsReady {
		log.Printf("called PlacementMayBeTaken in weird state: %s", p.State)
		return false
	}

	r := p.Range()

	switch r.State {
	case RsActive:
		var replacement *Placement
		for _, p2 := range r.Placements {
			if p2.IsReplacing == p.NodeID {
				replacement = p2
				break
			}
		}

		// p wants to be moved, but no replacement placement has been created yet.
		// Not sure how we ended up here, but it's valid.
		if replacement == nil {
			return false
		}

		if replacement.State == PsPrepared {
			return true
		}
	case RsSubsuming:

		// Exit early if any of the child ranges don't have enough replicas
		// in PsPrepared.
		for _, rc := range ks.Children(r) {
			n := 0
			for _, pc := range rc.Placements {
				if pc.State == PsPrepared {
					n += 1
				}
			}
			if n < rc.MinReady() {
				//log.Printf("child range has insufficient prepared placements: %s", rID)
				return false
			}
		}

		// All placements in all child ranges are prepared, i.e. ready to become
		// Ready. So we can become Taken, to relinquish all the keys in range.
		log.Printf("PlacementMayBeTaken: true (r=%s)", r)
		return true
	}

	return false
}

func (ks *Keyspace) PlacementMayBeDropped(p *Placement) error {

	// Sanity check.
	if p.State != PsTaken {
		return fmt.Errorf("placment not in PsTaken")
	}

	r := p.Range()
	switch r.State {
	case RsActive:

		// TODO: Move this (same in MayBeTaken) to r.ReplacementFor or something.
		var replacement *Placement
		for _, p2 := range r.Placements {
			if p2.IsReplacing == p.NodeID {
				replacement = p2
				break
			}
		}

		// p is in Taken, but no replacement exists? This should not have happened.
		// Maybe placing the replacement failed and we gave up?
		//
		// TODO: This should cause the placement to be *Untaken*. Maybe update this
		//       method to return the next action, i.e. Drop or Untake?
		//
		if replacement == nil {
			return fmt.Errorf("placement in PsTaken with no replacement")
		}

		if replacement.State != PsReady {
			return fmt.Errorf("replacement not PsReady; is %s", replacement.State)
		}

		return nil

	case RsSubsuming:

		// Can't drop until all of the child ranges have enough placements in
		// Ready state. They might still need the contents of this parent range
		// to make themselves ready. (Or we might want to abort the split and
		// move this parent range back to ready; not implemented yet.)
		for _, cr := range ks.Children(r) {
			ready := 0
			for _, cp := range cr.Placements {
				if cp.State == PsReady {
					ready += 1
				}
			}
			if ready < cr.MinReady() {
				return fmt.Errorf("child range has too few ready placements (want=%d, got=%d)", cr.MinReady(), ready)
			}
		}

		return nil

	default:
		return fmt.Errorf("unexpected state (s=%v)", r.State)
	}
}

func (ks *Keyspace) RangeCanBeObsoleted(r *Range) error {
	if r.State != RsSubsuming {
		// This should not be called in any other state.
		return fmt.Errorf("range not in RsSubsuming")
	}

	if l := len(r.Placements); l > 0 {
		return fmt.Errorf("has placements (n=%d)", l)
	}

	return nil
}

func (ks *Keyspace) Split(r *Range, k Key) error {

	// Balancer already holds the lock.
	//ks.mu.Lock()
	//defer ks.mu.Unlock()

	if k == ZeroKey {
		return fmt.Errorf("can't split on zero key")
	}

	if r.State != RsActive {
		return errors.New("can't split non-active range")
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
	err := r.toState(RsSubsuming, ks)
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

func (ks *Keyspace) Children(r *Range) []*Range {
	children := make([]*Range, len(r.Children))

	for i, rID := range r.Children {
		rp, err := ks.Get(rID)

		if err != nil {
			// This is actually a pretty major problem
			log.Printf("range has invalid child: %s (r=%s)", rID, r)
			continue
		}

		children[i] = rp
	}

	return children
}

//
//
//
//
///
// ---- old stuff below ----
//
//
//
//
//

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
		pers: ks.pers,
		Meta: Meta{
			Ident: ks.maxIdent,
		},

		// Born active, to be placed right away.
		// TODO: Maybe we need a pending state first? Do the parent ranges need
		//       to do anything else after this range is created but before it's
		//       placed?
		State: RsActive,

		// Starts dirty, because it hasn't been persisted yet.
		dirty: true,
	}

	return r
}

func (ks *Keyspace) RangesByState(s RangeState) []*Range {
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
		for i, p := range r.Placements {
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
		for i, p := range r.Placements {
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

// Len returns the number of ranges.
// This is mostly for testing, maybe remove it.
func (ks *Keyspace) Len() int {
	return len(ks.ranges)
}

// RangeToState tries to move the given range into the given state.
// TODO: Can we drop this and let range state transitions happen via Placement?
func (ks *Keyspace) RangeToState(rng *Range, state RangeState) error {
	// Balancer already has lock.
	// TODO: Verify this somehow?l
	//ks.mu.Lock()
	//defer ks.mu.Unlock()

	err := rng.toState(state, ks)
	if err != nil {
		return err
	}

	return ks.mustPersistDirtyRanges()
}

// Clear the current placement and mark the range as Obsolete. This should be
// called when a range is dropped after a split or join.
func (ks *Keyspace) DropPlacement(r *Range) error {
	panic("not implemented; see 839595a")
}

func (ks *Keyspace) PlacementToState(p *Placement, state PlacementState) error {
	//ks.mu.Lock()
	//defer ks.mu.Unlock()

	err := p.toState(state)
	if err != nil {
		panic(fmt.Sprintf("PlacementToState: %v", err))
		//return err
	}

	//p.rang.placementStateChanged(ks)
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

// Caller must hold the keyspace lock.
func (ks *Keyspace) JoinTwo(one *Range, two *Range) (*Range, error) {
	//ks.mu.Lock()
	//defer ks.mu.Unlock()

	for _, r := range []*Range{one, two} {
		if r.State != RsActive {
			return nil, errors.New("can't join non-ready ranges")
		}

		// This should not be possible. Panic?
		if len(r.Children) > 0 {
			return nil, fmt.Errorf("range %s already has %d children", r, len(r.Children))
		}
	}

	// TODO: Probably fine to transparently flip them around in this case.
	if one.Meta.End != two.Meta.Start {
		return nil, fmt.Errorf("not adjacent: %s, %s", one, two)
	}

	for _, r := range []*Range{one, two} {
		err := r.toState(RsSubsuming, ks)
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

	// if r.State != Obsolete {
	// 	return errors.New("can't discard non-obsolete range")
	// }

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
