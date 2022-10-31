package keyspace

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/adammck/ranger/pkg/ranje"
)

// Operations returns the operations (splits, joins) which are currently in
// progress. Invalidated after any kind of transformation.
func (ks *Keyspace) Operations() ([]*Operation, error) {

	// Build a set of active ranges to consider. This is churning through the
	// whole range history, but only really needs the leaf ranges. Keep an index
	// of those in the keyspace.
	ranges := map[ranje.Ident]*ranje.Range{}
	for _, r := range ks.ranges {
		if r.State == ranje.RsActive {
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
	sort := ranje.Ident(math.MaxInt)

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

func (op *Operation) isDirection(d dir, rID ranje.Ident) bool {
	for _, rr := range op.direction(d) {
		if rID == rr.Meta.Ident {
			return true
		}
	}

	return false
}

func (op *Operation) Destinations() []*ranje.Range {
	return op.direction(Dest)
}

func (op *Operation) IsDestination(rID ranje.Ident) bool {
	return op.isDirection(Dest, rID)
}

func (op *Operation) Sources() []*ranje.Range {
	return op.direction(Source)
}

func (op *Operation) IsSource(rID ranje.Ident) bool {
	return op.isDirection(Source, rID)
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

// TODO: Remove this. It's only used in the tests.
func (op *Operation) IsParent(rID ranje.Ident) bool {
	for _, rr := range op.parents {
		if rID == rr.Meta.Ident {
			return true
		}
	}

	return false
}

func (op *Operation) IsChild(rID ranje.Ident) bool {
	for _, rr := range op.children {
		if rID == rr.Meta.Ident {
			return true
		}
	}

	return false
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
		err := ks.RangeToState(r, ranje.RsObsolete)
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
			if pc.FailedDeactivate {
				out = true
			}
		}
	}

	// If any of the children failed to activate (regardless of how many of them
	// there are), we must deactivate them all and reactivate the parents to
	// ensure no gaps while we give the failed range(s) to some other node.
	for _, rc := range children {
		for _, pc := range rc.Placements {
			if pc.FailedActivate {
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
	if r.State != ranje.RsActive {
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
	var sp ranje.RangeState

	parents := make([]*ranje.Range, len(r.Parents))
	for i := range r.Parents {
		rp, err := ks.Get(r.Parents[i])
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

	// If the parents are all obsolete, then there is no operation in progress.
	// This is the case most of the time.
	if sp == ranje.RsObsolete {
		return nil, ErrObsoleteParents
	}

	if sp == ranje.RsSplitting {
		if len(parents) != 1 {
			return nil, fmt.Errorf(
				"too many parents for split of rID=%d: %d",
				r.Meta.Ident, len(parents))
		}

		rp := parents[0]

		// Fetch all the children of the parent range. This will include the
		// range this function was called with and its siblings.
		children := make([]*ranje.Range, len(rp.Children))
		for i := range rp.Children {
			children[i], err = ks.Get(rp.Children[i])
			if err != nil {
				return nil, err
			}
		}

		return NewOperation(parents, children), nil
	}

	if sp != ranje.RsJoining {
		return nil, fmt.Errorf(
			"unexpected state for parents of rID=%d: %s",
			r.Meta.Ident, sp)
	}

	// We could return the JoinOp now, but just do one more sanity check that
	// all of the parents have exactly one child, which is the range we started
	// with. Otherwise we might be trying to do some kind of weird future op.
	for i := range parents {
		if len(parents[i].Children) != 1 {
			return nil, fmt.Errorf(
				"wrong number of children for parent of rID=%d: %d",
				r.Meta.Ident, len(parents[i].Children))
		}
		if parents[i].Children[0] != r.Meta.Ident {
			return nil, fmt.Errorf(
				"wrong child of rID=%d: %d",
				parents[i].Meta.Ident, parents[i].Children[0])
		}
	}

	return NewOperation(parents, []*ranje.Range{r}), nil
}
