package keyspace

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
)

// Operations returns the operations (splits, joins) which are currently in
// progress. Invalidated after any kind of transformation.
func (ks *Keyspace) Operations() ([]*Operation, error) {

	// Build a set of active ranges to consider. This is churning through the
	// whole range history, but only really needs the leaf ranges. Keep an index
	// of those in the keyspace.
	ranges := map[api.Ident]*ranje.Range{}
	for _, r := range ks.ranges {
		if r.State == api.RsActive {
			ranges[r.Meta.Ident] = r
		}
	}

	ops := []*Operation{}
	for _, r := range ranges {
		// Construct the operation from this range, however it's connected.
		op, err := opFromRange(ks, r)
		if err != nil {
			if err == ErrNoParents || err == ErrObsoleteParents {
				continue
			} else {
				return nil, fmt.Errorf("finding operations: %w", err)
			}
		}
		ops = append(ops, op)

		// Remove all of the ranges which are part of this operation from the
		// list, so we don't waste time constructing duplicate operations.
		for _, r := range op.Ranges() {
			delete(ranges, r.Meta.Ident)
		}
	}

	// Return in arbitrary but stable order.
	sort.Slice(ops, func(a, b int) bool {
		return ops[a].sort < ops[b].sort
	})

	return ops, nil
}

type Operation struct {
	parents  []*ranje.Range
	children []*ranje.Range

	// Whether the operation should be moving forwards (false, default), such
	// that ownership moves towards child ranges, or backwards (true) such that
	// the parents receive it. This is updated by CheckRecall.
	recall bool

	// Calculated at init just to provide sort stability for tests.
	sort int
}

func NewOperation(parents, children []*ranje.Range) *Operation {
	sort := api.Ident(math.MaxInt)

	// Sort by the lowest range ID in each operation.
	for _, rs := range [][]*ranje.Range{parents, children} {
		for _, r := range rs {
			if r.Meta.Ident < sort {
				sort = r.Meta.Ident
			}
		}
	}

	recall := isRecalling(parents, children)

	return &Operation{
		parents:  parents,
		children: children,
		sort:     int(sort),
		recall:   recall,
	}
}

func (op *Operation) TestString() string {
	p := make([]string, len(op.parents))
	for i := range op.parents {
		p[i] = op.parents[i].Meta.Ident.String()
	}

	c := make([]string, len(op.children))
	for i := range op.children {
		c[i] = op.children[i].Meta.Ident.String()
	}

	// Recall controls the direction of flow. Normally it's parents -> children,
	// but when inverted it's parents <- children.
	direction := "->"
	if op.recall {
		direction = "<-"
	}

	// String to describe the "shape" of the operation (split or join), to make
	// things a bit clearer in the common cases. This is just a heuristic; don't
	// use it for anything important.
	shape := "Operation"
	if len(p) == 1 && len(c) > 1 {
		shape = "Split"
	} else if len(p) > 1 && len(c) == 1 {
		shape = "Join"
	}

	return fmt.Sprintf(
		"{%s %s %s %s}", // {Split 1 -> 2,3}
		shape,
		strings.Join(p, ","),
		direction,
		strings.Join(c, ","))
}

type dir uint8

const (
	Dest   dir = iota
	Source dir = iota
)

func (op *Operation) direction(d dir) []*ranje.Range {
	if (d == Dest) != op.recall {
		return op.children
	} else {
		return op.parents
	}
}

func (op *Operation) isDirection(d dir, rID api.Ident) bool {
	for _, rr := range op.direction(d) {
		if rID == rr.Meta.Ident {
			return true
		}
	}

	return false
}

func (op *Operation) Ranges() []*ranje.Range {
	out := make([]*ranje.Range, len(op.parents)+len(op.children))

	// Copy the first half as-is.
	copy(out, op.parents)

	// The second half offset by the width of the first part.
	for i, r := range op.children {
		out[len(op.parents)+i] = r
	}

	return out
}

// CheckComplete checks the status of the operation, and if complete, marks the
// parent ranges as obsolete and returns true.
// TODO: Maybe just make the ks a field on Operation.
func (op *Operation) CheckComplete(ks *Keyspace) (bool, error) {
	for _, r := range op.parents {
		if l := len(r.Placements); l > 0 {
			return false, nil
		}
	}

	for _, r := range op.parents {
		err := ks.RangeToState(r, api.RsObsolete)
		if err != nil {
			return false, fmt.Errorf("while completing operation: %w", err)
		}
	}

	return true, nil
}

// isRecalling returns true if the given parent and child ranges should result
// in a recall, i.e. if this operation should (temporarily) flow in reverse, by
// deactivating the children and activating the parents.
func isRecalling(parents, children []*ranje.Range) bool {
	out := false

	// If any of the parents failed to deactivate, others may have succeeded and
	// are now inactive, so we must roll back to reactivate them to ensure no
	// gaps while we wait for an operator (or node crash).
	//
	// TODO: Is this necessary if there's only one parent? What can even be done
	//       except the split becoming wedged? TestSplitFailure_PrepareDropRange
	//       will maybe untangle this.
	for _, rc := range parents {
		for _, pc := range rc.Placements {
			if pc.Failed(api.Take) {
				out = true
			}
		}
	}

	// If any of the children failed to activate (regardless of how many of them
	// there are), we must deactivate them all and reactivate the parents to
	// ensure no gaps while we give the failed range(s) to some other node.
	for _, rc := range children {
		for _, pc := range rc.Placements {
			if pc.Failed(api.Serve) {
				out = true
			}
		}
	}

	// What of FailedGive (or FailedPrepareAddRange, or FailedLoad or whatever
	// I'm calling it today), and of FailedDrop?
	//
	// Firstly, there is no FailedGive; the placement is just destroyed if it
	// isn't accepted by the node. Secondly, even if there were, we don't have
	// to roll anything back to handle it. Placement happens before the parent
	// ranges are deactivated, so can be re-tried as many times as necessary
	// without causing any drama. Only the deactivate/activate step causes
	// unavailability, so we must tap-dance around it.
	//
	// Regarding FailedDrop, likewise, it doesn't matter. The handover from
	// parent to child ranges has already finished, it's just cleanup of the
	// parents which is causing problems. We don't want to roll back because of
	// that, and we couldn't do it reliably anyway, because some of the parents
	// may have successfully been dropped already.

	return out
}

var ErrNoParents = errors.New("given range has no parents")
var ErrObsoleteParents = errors.New("given range has obsolete parents")

func opFromRange(ks *Keyspace, r *ranje.Range) (op *Operation, err error) {
	if r.State != api.RsActive {
		panic("bug: called opFromRange with non-active range")
	}

	// Special case: Zero parents means this is a genesis range. So long as it's
	// active (above), that's fine. Otherwise we would expect all leaf ranges
	// to have at least one parent.
	pl := len(r.Parents)
	if pl == 0 {
		return nil, ErrNoParents
	}

	// Keep track of the state of parent ranges as we iterate through them. (If
	// we find any that don't match, the keyspace is borked.)
	var sp api.RangeState

	// Collect all of the parent ranges involved in this operation.
	parents := make([]*ranje.Range, len(r.Parents))
	for i, prID := range r.Parents {
		rp, err := ks.Get(prID)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			sp = rp.State
		} else {
			if rp.State != sp {
				return nil, fmt.Errorf(
					"parents of rID=%d in inconsistent state; got %s and %s",
					r.Meta.Ident, sp, rp.State)
			}
		}

		parents[i] = rp
	}

	// Also collect all of the child ranges, which will include whatever range
	// this func was called with. In simple cases (split/join) there'll either
	// be one parent or one child, and more than one of the other. We might
	// support more complex (n:m) cases one day.
	children := []*ranje.Range{} // length unknown
	seen := map[api.Ident]struct{}{}
	for i := range parents {
		for _, cID := range parents[i].Children {
			if _, ok := seen[cID]; ok {
				continue
			}
			seen[cID] = struct{}{}
			c, err := ks.Get(cID)
			if err != nil {
				return nil, err
			}

			children = append(children, c)
		}
	}

	// If the parents are all obsolete, then there is no operation in progress.
	// This is the case most of the time.
	if sp == api.RsObsolete {
		return nil, ErrObsoleteParents
	}

	if sp != api.RsSubsuming {
		return nil, fmt.Errorf(
			"unexpected state for parents of rID=%d: %s",
			r.Meta.Ident, sp)
	}

	// Reject complex operations for now.
	// TODO: Remove this; it might work already!
	if !(len(parents) == 1 && len(children) > 1) && !(len(children) == 1 && len(parents) > 1) {
		return nil, fmt.Errorf(
			"op is not a simple split or a join; rID=%d, p=%d, c=%d",
			r.Meta.Ident, len(parents), len(children))
	}

	return NewOperation(parents, children), nil
}

// MayActivate returns whether the given placement is permitted to move from
// PsInactive to PsActive.
func (op *Operation) MayActivate(p *ranje.Placement, r *ranje.Range) error {
	// Beware! This is sometimes called with a nil operation!
	// TODO: Figure out what to do for op-less placement judgments.

	if p.StateCurrent != api.PsInactive {
		return fmt.Errorf("placment not in api.PsInactive")
	}

	// If this placement has been given up on, it's destined to be dropped
	// rather than served. We might have tried to serve it and failed.
	if p.Failed(api.Serve) {
		return fmt.Errorf("gave up")
	}

	// Count how many PsActive placements this range has. If it's fewer than the
	// MinReady (i.e. the minimum number of active placements wanted), then this
	// placement may become active.
	n := 0
	for _, pp := range r.Placements {
		if pp.StateCurrent == api.PsActive {
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
			if !other.Failed(api.Serve) {
				return fmt.Errorf("will be replaced by sibling")
			}
		}

		return nil
	}

	if op.isDirection(Dest, r.Meta.Ident) {
		// Wait until all of the placements in the back side have been
		// deactivated. Otherwise, there will be overlaps.
		for _, rp := range op.direction(Source) {
			for _, pp := range rp.Placements {
				if pp.StateCurrent == api.PsActive {
					return fmt.Errorf("parent placement is PsActive")
				}
			}
		}
	}

	if op.isDirection(Source, r.Meta.Ident) {
		return fmt.Errorf("never activate backside")
	}

	return nil
}

// MayDeactivate returns true if the given placement is permitted to move from
// PsActive to PsInactive.
func (op *Operation) MayDeactivate(p *ranje.Placement, r *ranje.Range) error {
	// Beware! This is sometimes called with a nil operation!
	// TODO: Figure out what to do for op-less placement judgments.

	if p.StateCurrent != api.PsActive {
		return fmt.Errorf("placment not in api.PsActive")
	}

	if p.Failed(api.Take) {
		return fmt.Errorf("gave up")
	}

	if op == nil {

		// If this placement is being replaced by another...
		if other := replacementFor(p); other != nil {

			// There is another placement replacing this one, but we've given up on
			// it, so will destroy that (the replacement) rather than taking this
			// one. We might have already taken this one once, and then reverted it
			// back because the replacement failed to become ready.
			if other.Failed(api.Serve) {
				return fmt.Errorf("replacement has given up")
			}

			if other.StateCurrent != api.PsInactive {
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

	if op.isDirection(Dest, r.Meta.Ident) {
		return fmt.Errorf("no problem")
	}

	if op.isDirection(Source, r.Meta.Ident) {

		// Can't deactivate if there aren't enough child placements waiting in
		// Inactive.
		for _, rc := range op.direction(Dest) {

			n := 0
			for _, pc := range rc.Placements {
				if pc.StateCurrent == api.PsInactive {
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

// isChild returns true if the given range ID is one of the child ranges in this
// operation. This mostly shouldn't be used. Special case just for MayDrop.
func (op *Operation) isChild(rID api.Ident) bool {
	for _, rr := range op.children {
		if rID == rr.Meta.Ident {
			return true
		}
	}

	return false
}

// MayDrop returns true if the given placement is permitted to move from
// PsInactive to PsDropped (and then be destroyed).
func (op *Operation) MayDrop(p *ranje.Placement, r *ranje.Range) error {
	// Beware! This is sometimes called with a nil operation!
	// TODO: Figure out what to do for op-less placement judgments.

	if p.StateCurrent != api.PsInactive {
		return fmt.Errorf("placment not in api.PsInactive")
	}

	if op == nil {
		// Is this placement being replaced by some other? Can drop as soon as
		// that other placement becomes active.
		if other := replacementFor(p); other != nil {
			if other.StateCurrent != api.PsActive {
				return fmt.Errorf("replacement not api.PsActive; is %s", other.StateCurrent)
			}

			return nil
		}

		// Is this placement replacing some other?
		if other := replacedBy(p); other != nil {

			// If this placement (the replacement) has failed to activate, the
			// other placement should be reactivated and this one should be
			// dropped. In order to make that a bit safer and more orderly,
			// delay the drop until after the other one has reactivated.
			if p.Failed(api.Serve) {
				if other.StateCurrent == api.PsActive {
					return nil
				} else {
					return fmt.Errorf("won't drop aborted placement until original is reactivated")
				}
			}

			// If the other placement has failed to deactivate, might as well
			// drop this one (the replacements) while an operator intervenes.
			if other.Failed(api.Take) {
				return nil
			}
		}

		// Not replacing any other placement, just floating around...? Not sure
		// what's going on here, but the placement has probably been placed and
		// is about to be activated, so don't drop it unless that's already been
		// tried and failed.

		if p.Failed(api.Serve) {
			return nil
		}

		return fmt.Errorf("no reason to drop")
	}

	if op.isDirection(Dest, r.Meta.Ident) {
		if p.Failed(api.Serve) {
			return nil
		}

		return fmt.Errorf("not dropping inactive child; probably on the way to activate")
	}

	if op.isDirection(Source, r.Meta.Ident) {

		// Can't drop until all of the destination ranges have enough active
		// placements. They might still need the contents of this parent range
		// to make themselves ready.
		for _, r2 := range op.direction(Dest) {

			active := 0
			for _, p2 := range r2.Placements {
				if p2.StateCurrent == api.PsActive {
					active += 1
				}
				if p2.Failed(api.Serve) {
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

		if op.isChild(r.Meta.Ident) {
			if p.Failed(api.Serve) {
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
