package keyspace

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/persister"
	"github.com/adammck/ranger/pkg/ranje"
)

// Keyspace is an overlapping set of ranges which cover all of the possible
// values in the space. Any value is covered by either one or two ranges: one
// range in the steady-state, where nothing is being moved around, and two
// ranges while rebalancing is in progress.
type Keyspace struct {
	cfg      config.Config
	pers     persister.Persister
	ranges   []*ranje.Range // TODO: don't be dumb, use an interval tree
	mu       sync.RWMutex
	maxIdent ranje.Ident
}

func New(cfg config.Config, persister persister.Persister) (*Keyspace, error) {
	ks := &Keyspace{
		cfg:  cfg,
		pers: persister,
	}

	ranges, err := persister.GetRanges()
	if err != nil {
		return nil, err
	}

	log.Printf("got %d ranges from store\n", len(ranges))

	// Special case: There are no ranges in the store. We are bootstrapping the
	// keyspace from scratch, so start with a singe range that covers all keys.
	if len(ranges) == 0 {
		r := ks.Range()
		ks.ranges = []*ranje.Range{r}
		ks.pers.PutRanges(ks.ranges)
		return ks, nil
	}

	ks.ranges = ranges
	for _, r := range ks.ranges {

		// Repair the range.
		// TODO: Anything left to do here?

		// Repair the placements
		for _, p := range r.Placements {
			p.Repair(r)
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

	return ks, nil
}

// LogString is only used by tests.
// TODO: Move it to the tests!
func (ks *Keyspace) LogString() string {
	s := make([]string, len(ks.ranges))

	for i, r := range ks.ranges {
		s[i] = r.LogString()
	}

	return strings.Join(s, " ")
}

// SanityCheck returns an error if the curernt range state isn't sane. It does
// nothing to try to rectify the situaton. This method mostly compensates for
// the fact that I'm storing all these ranges in a single flat slice.
func (ks *Keyspace) SanityCheck() error {

	// Check for no duplicate range IDs.

	seen := map[ranje.Ident]struct{}{}
	for _, r := range ks.ranges {
		if _, ok := seen[r.Meta.Ident]; ok {
			return fmt.Errorf("duplicate range ID (i=%d)", r.Meta.Ident)
		}
		seen[r.Meta.Ident] = struct{}{}
	}

	// Check for any ranges in an unknown state.
	for i := range ks.ranges {
		if ks.ranges[i].State == ranje.RsUnknown {
			return fmt.Errorf("range in unknown state (rID=%v)", ks.ranges[i].Meta.Ident)
		}
	}

	// Check that leaf ranges (i.e. those with no children) cover the entire
	// keyspace with no overlaps. This must always be the case; we only persist
	// valid configurations, and do it transactionally.

	leafs := []*ranje.Range{}
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
		if r.State != ranje.RsActive {
			return fmt.Errorf("non-active leaf range with no children (rID=%v)", r.Meta.Ident)
		}

		if i == 0 { // first range
			if r.Meta.Start != ranje.ZeroKey {
				return fmt.Errorf("first leaf range did not start with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not first range
			if r.Meta.Start == ranje.ZeroKey {
				return fmt.Errorf("non-first leaf range started with zero key (rID=%d)", r.Meta.Ident)
			}
			if r.Meta.Start != leafs[i-1].Meta.End {
				return fmt.Errorf("leaf range does not begin at prior leaf range end (i=%d)", i)
			}
		}
		if i == len(leafs)-1 { // last range
			if r.Meta.End != ranje.ZeroKey {
				return fmt.Errorf("last leaf range did not end with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not last range
			if r.Meta.End == ranje.ZeroKey {
				return fmt.Errorf("non-last leaf range ended with zero key (rID=%d)", r.Meta.Ident)
			}
		}
	}

	return nil
}

func (ks *Keyspace) Ranges() ([]*ranje.Range, func()) {
	ks.mu.Lock()
	return ks.ranges, ks.mu.Unlock
}

// PlacementMayActivate returns whether the given placement is permitted to
// advance from ranje.PsInactive to ranje.PsActive.
func (ks *Keyspace) PlacementMayActivate(p *ranje.Placement, r *ranje.Range, op *Operation) error {
	if p.State != ranje.PsInactive {
		return fmt.Errorf("placment not in ranje.PsInactive")
	}

	// If this placement has been given up on, it's destined to be dropped
	// rather than served. We might have tried to serve it and failed.
	if p.FailedActivate {
		return fmt.Errorf("gave up")
	}

	// Count how many PsActive placements this range has. If it's fewer than the
	// MinReady (i.e. the minimum number of active placements wanted), then this
	// placement may become active.
	n := 0
	for _, pp := range r.Placements {
		if pp.State == ranje.PsActive {
			n += 1
		}
	}

	// TODO: Isn't this bounded by MaxReady, instead?
	if n >= r.MinReady() {
		return fmt.Errorf("too many ready placements (n=%d, MinReady=%d)", n, r.MinReady())
	}

	if op == nil {

		// If one of the sibling placements (on the same range) claims to be
		// replacing this range, then it mustn't become ready (unless that sibling
		// has given up). It was probably just moved from ready to Idle so that the
		// sibling could become ready.
		if other := replacementFor(p); other != nil {
			if !other.FailedActivate {
				return fmt.Errorf("will be replaced by sibling")
			}
		}

		return nil
	}

	if op.IsDestination(r.Meta.Ident) {
		// Wait until all of the placements in the back side have been
		// deactivated. Otherwise, there will be overlaps.
		for _, rp := range op.Sources() {
			for _, pp := range rp.Placements {
				if pp.State == ranje.PsActive {
					return fmt.Errorf("parent placement is PsActive")
				}
			}
		}
	}

	if op.IsSource(r.Meta.Ident) {
		return fmt.Errorf("never activate backside")
	}

	return nil
}

// PlacementMayDeactivate returns true if the given placement is permitted to
// move from PsActive to PsInactive.
func (ks *Keyspace) PlacementMayDeactivate(p *ranje.Placement, r *ranje.Range, op *Operation) error {
	if p.State != ranje.PsActive {
		return fmt.Errorf("placment not in ranje.PsActive")
	}

	if p.FailedDeactivate {
		return fmt.Errorf("gave up")
	}

	if op == nil {

		// If this placement is being replaced by another...
		if other := replacementFor(p); other != nil {

			// There is another placement replacing this one, but we've given up on
			// it, so will destroy that (the replacement) rather than taking this
			// one. We might have already taken this one once, and then reverted it
			// back because the replacement failed to become ready.
			if other.FailedActivate {
				return fmt.Errorf("replacement has given up")
			}

			if other.State != ranje.PsInactive {
				return fmt.Errorf("replacement is not inactive")
			}

			return nil
		}

		// Is this placement replacing some other? It's already in active, so
		// the other must have already become inactive. There is no reason we'd
		// turn around now.
		if other := replacedBy(p); other != nil {
			return fmt.Errorf("replacing other")
		}

		// Otherwise, no operation is in progress and the placement isn't being
		// replaced, so there is no reason that we'd deactivate.
		return fmt.Errorf("no reason")
	}

	if op.IsDestination(r.Meta.Ident) {
		return fmt.Errorf("no problem")
	}

	if op.IsSource(r.Meta.Ident) {

		// Can't deactivate if there aren't enough child placements waiting in
		// Inactive.
		for _, rc := range op.Destinations() {

			n := 0
			for _, pc := range rc.Placements {
				if pc.State == ranje.PsInactive {
					n += 1
				}
			}

			if n < rc.MinReady() {
				return fmt.Errorf("not enough inactive children")
			}
		}

		return nil
	}

	// Shouldn't reach here. This is a bug.
	panic("unsure whether to deactivate")
}

func (ks *Keyspace) PlacementMayDrop(p *ranje.Placement, r *ranje.Range, op *Operation) error {
	if p.State != ranje.PsInactive {
		return fmt.Errorf("placment not in ranje.PsInactive")
	}

	if op == nil {
		// Is this placement being replaced by some other? Can drop as soon as
		// that other placement becomes active.
		if other := replacementFor(p); other != nil {
			if other.State != ranje.PsActive {
				return fmt.Errorf("replacement not ranje.PsActive; is %s", other.State)
			}

			return nil
		}

		// Is this placement replacing some other?
		if other := replacedBy(p); other != nil {

			// If this placement (the replacement) has failed to activate, the
			// other placement should be reactivated and this one should be
			// dropped. In order to make that a bit safer and more orderly,
			// delay the drop until after the other one has reactivated.
			if p.FailedActivate {
				if other.State == ranje.PsActive {
					return nil
				} else {
					return fmt.Errorf("won't drop aborted placement until original is reactivated")
				}
			}

			// If the other placement has failed to deactivate, might as well
			// drop this one (the replacements) while an operator intervenes.
			if other.FailedDeactivate {
				return nil
			}
		}

		// Not replacing any other placement, just floating around...? Not sure
		// what's going on here, but the placement has probably been placed and
		// is about to be activated, so don't drop it unless that's already been
		// tried and failed.

		if p.FailedActivate {
			return nil
		}

		return fmt.Errorf("no reason to drop")
	}

	if op.IsDestination(r.Meta.Ident) {
		if p.FailedActivate {
			return nil
		}

		return fmt.Errorf("not dropping inactive child; probably on the way to activate")
	}

	if op.IsSource(r.Meta.Ident) {

		// Can't drop until all of the destination ranges have enough active
		// placements. They might still need the contents of this parent range
		// to make themselves ready.
		for _, r2 := range op.Destinations() {

			active := 0
			for _, p2 := range r2.Placements {
				if p2.State == ranje.PsActive {
					active += 1
				}
				if p2.FailedActivate {
					return fmt.Errorf("child range has placement given up, which will be dropped")
				}
			}
			if active < r2.MinReady() {
				return fmt.Errorf("child range has too few active placements (want=%d, got=%d)", r2.MinReady(), active)
			}
		}

		// Special case: if we are rolling back -- both source and child -- and
		// this placement has not failed, don't drop it. Leave it hanging around
		// in inactive, instead. There's nothing wrong with it, so we don't need
		// to drop it. We're probably rolling back because of a problem with one
		// of the other child ranges.
		//
		// If this placement *did* fail, it's probably the reason we're rolling
		// back, so do drop it.

		if op.IsChild(r.Meta.Ident) {
			if p.FailedActivate {
				return nil
			}

			return fmt.Errorf("not dropping non-failed child")
		}

		return nil
	}

	return fmt.Errorf("not a child, not a parent? rID=%v", r.Meta.Ident)
}

func replacedBy(p *ranje.Placement) *ranje.Placement {
	var out *ranje.Placement

	for _, pp := range p.Range().Placements {
		if p.IsReplacing == pp.NodeID {
			out = pp
			break
		}
	}

	return out
}

func replacementFor(p *ranje.Placement) *ranje.Placement {
	var out *ranje.Placement

	for _, pp := range p.Range().Placements {
		if pp.IsReplacing == p.NodeID {
			out = pp
			break
		}
	}

	return out
}

func (ks *Keyspace) RangeCanBeObsoleted(r *ranje.Range, op *Operation) error {
	if r.State != ranje.RsSubsuming {
		// This should not be called in any other state.
		return fmt.Errorf("range not in RsSubsuming (was: %v)", r.State)
	}

	if l := len(r.Placements); l > 0 {
		return fmt.Errorf("has placements (n=%d)", l)
	}

	return nil
}

func (ks *Keyspace) Split(r *ranje.Range, k ranje.Key) (one *ranje.Range, two *ranje.Range, err error) {
	if k == ranje.ZeroKey {
		err = fmt.Errorf("can't split on zero key")
		return
	}

	if r.State != ranje.RsActive {
		err = errors.New("can't split non-active range")
		return
	}

	// This should not be possible. Panic?
	if len(r.Children) > 0 {
		err = fmt.Errorf("range %s already has %d children", r, len(r.Children))
		return
	}

	if !r.Meta.Contains(k) {
		err = fmt.Errorf("range %s does not contain key: %s", r, k)
		return
	}

	if k == r.Meta.Start {
		err = fmt.Errorf("range %s starts with key: %s", r, k)
		return
	}

	// Change the state of the splitting range directly via Range.toState rather
	// than Keyspace.ToState as (usually!) recommended, because we don't want
	// to persist the change until the two new ranges have been created, below.
	err = r.ToState(ranje.RsSubsuming)
	if err != nil {
		// The error is clear enough, no need to wrap it.
		return
	}

	one = ks.Range()
	one.Meta.Start = r.Meta.Start
	one.Meta.End = k
	one.Parents = []ranje.Ident{r.Meta.Ident}

	two = ks.Range()
	two.Meta.Start = k
	two.Meta.End = r.Meta.End
	two.Parents = []ranje.Ident{r.Meta.Ident}

	// append to the end of the ranges
	// TODO: Insert the children after the parent, not at the end!
	ks.ranges = append(ks.ranges, one)
	ks.ranges = append(ks.ranges, two)

	r.Children = []ranje.Ident{
		one.Meta.Ident,
		two.Meta.Ident,
	}

	// Persist all three ranges.
	ks.mustPersistDirtyRanges()

	return
}

// Get returns a range by its ident, or an error if no such range exists.
// TODO: Allow getting by other things.
// TODO: Should this lock ranges? Or the caller do it?
func (ks *Keyspace) Get(id ranje.Ident) (*ranje.Range, error) {
	for _, r := range ks.ranges {
		if r.Meta.Ident == id {
			return r, nil
		}
	}

	return nil, fmt.Errorf("no such range: %s", id.String())
}

// Range returns a new range with the next available ident. This is the only
// way that a Range should be constructed. Callers are responsible for calling
// mustPersistDirtyRanges to persist the new range after mutating it.
func (ks *Keyspace) Range() *ranje.Range {
	ks.maxIdent += 1
	return ranje.NewRange(ks.maxIdent)
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

type PBNID struct {
	Range     *ranje.Range
	Placement *ranje.Placement
	Position  uint8
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

// RangeToState tries to move the given range into the given state.
// TODO: This is currently only used to move ranges into Obsolete after they
//       have been subsumed. Can we replace this with *ObsoleteRange*?
func (ks *Keyspace) RangeToState(r *ranje.Range, state ranje.RangeState) error {
	// Orchestrator already has lock.
	// TODO: Verify this somehow?

	err := r.ToState(state)
	if err != nil {
		return err
	}

	return ks.mustPersistDirtyRanges()
}

// Callers don't bother checking the error we return, so we panic instead.
// TODO: Update callers to check the error!
func (ks *Keyspace) PlacementToState(p *ranje.Placement, state ranje.PlacementState) error {
	err := p.ToState(state)
	if err != nil {
		panic(fmt.Sprintf("toState: %v", err))
	}

	err = ks.mustPersistDirtyRanges()
	if err != nil {
		panic(fmt.Sprintf("mustPersistDirtyRanges: %v", err))
	}

	return nil
}

// TODO: Return an error instead of panicking, and rename.
func (ks *Keyspace) mustPersistDirtyRanges() error {
	ranges := []*ranje.Range{}
	for _, r := range ks.ranges {
		if r.Dirty() {
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
func (ks *Keyspace) JoinTwo(one *ranje.Range, two *ranje.Range) (*ranje.Range, error) {
	for _, r := range []*ranje.Range{one, two} {
		if r.State != ranje.RsActive {
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

	for _, r := range []*ranje.Range{one, two} {
		err := r.ToState(ranje.RsSubsuming)
		if err != nil {
			// The error is clear enough, no need to wrap it.
			return nil, err
		}
	}

	three := ks.Range()
	three.Meta.Start = one.Meta.Start
	three.Meta.End = two.Meta.End
	three.Parents = []ranje.Ident{one.Meta.Ident, two.Meta.Ident}

	// Insert new range at the end.
	ks.ranges = append(ks.ranges, three)

	one.Children = []ranje.Ident{three.Meta.Ident}
	two.Children = []ranje.Ident{three.Meta.Ident}

	// Persist all three ranges atomically.
	ks.mustPersistDirtyRanges()

	return three, nil
}
