package keyspace

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/api"
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
	maxIdent api.Ident
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

	seen := map[api.Ident]struct{}{}
	for _, r := range ks.ranges {
		if _, ok := seen[r.Meta.Ident]; ok {
			return fmt.Errorf("duplicate range ID (i=%d)", r.Meta.Ident)
		}
		seen[r.Meta.Ident] = struct{}{}
	}

	// Check for any ranges in an unknown state.
	for i := range ks.ranges {
		if ks.ranges[i].State == api.RsUnknown {
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
		if r.State != api.RsActive {
			return fmt.Errorf("non-active leaf range with no children (rID=%v)", r.Meta.Ident)
		}

		if i == 0 { // first range
			if r.Meta.Start != api.ZeroKey {
				return fmt.Errorf("first leaf range did not start with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not first range
			if r.Meta.Start == api.ZeroKey {
				return fmt.Errorf("non-first leaf range started with zero key (rID=%d)", r.Meta.Ident)
			}
			if r.Meta.Start != leafs[i-1].Meta.End {
				return fmt.Errorf("leaf range does not begin at prior leaf range end (i=%d)", i)
			}
		}
		if i == len(leafs)-1 { // last range
			if r.Meta.End != api.ZeroKey {
				return fmt.Errorf("last leaf range did not end with zero key (rID=%d)", r.Meta.Ident)
			}
		} else { // not last range
			if r.Meta.End == api.ZeroKey {
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

func (ks *Keyspace) Split(r *ranje.Range, k api.Key) (one *ranje.Range, two *ranje.Range, err error) {
	if k == api.ZeroKey {
		err = fmt.Errorf("can't split on zero key")
		return
	}

	if r.State != api.RsActive {
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
	err = r.ToState(api.RsSubsuming)
	if err != nil {
		// The error is clear enough, no need to wrap it.
		return
	}

	one = ks.Range()
	one.Meta.Start = r.Meta.Start
	one.Meta.End = k
	one.Parents = []api.Ident{r.Meta.Ident}

	two = ks.Range()
	two.Meta.Start = k
	two.Meta.End = r.Meta.End
	two.Parents = []api.Ident{r.Meta.Ident}

	// append to the end of the ranges
	// TODO: Insert the children after the parent, not at the end!
	ks.ranges = append(ks.ranges, one)
	ks.ranges = append(ks.ranges, two)

	r.Children = []api.Ident{
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
func (ks *Keyspace) Get(id api.Ident) (*ranje.Range, error) {
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
func (ks *Keyspace) RangeToState(r *ranje.Range, state api.RangeState) error {
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
func (ks *Keyspace) PlacementToState(p *ranje.Placement, state api.PlacementState) error {
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
		if r.State != api.RsActive {
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
		err := r.ToState(api.RsSubsuming)
		if err != nil {
			// The error is clear enough, no need to wrap it.
			return nil, err
		}
	}

	three := ks.Range()
	three.Meta.Start = one.Meta.Start
	three.Meta.End = two.Meta.End
	three.Parents = []api.Ident{one.Meta.Ident, two.Meta.Ident}

	// Insert new range at the end.
	ks.ranges = append(ks.ranges, three)

	one.Children = []api.Ident{three.Meta.Ident}
	two.Children = []api.Ident{three.Meta.Ident}

	// Persist all three ranges atomically.
	ks.mustPersistDirtyRanges()

	return three, nil
}
