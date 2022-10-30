package keyspace

import (
	"errors"
	"fmt"
	"math"
	"sort"

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
			if err == ErrNoParents {
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

	return &Operation{
		parents:  parents,
		children: children,
		sort:     int(sort),
	}

}

func (op *Operation) Parents() []*ranje.Range {
	// TODO: Can this method be removed? Whatever callers doing moved in here.
	return op.parents
}

func (op *Operation) Children() []*ranje.Range {
	// TODO: Can this method be removed? Whatever callers doing moved in here.
	return op.children
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

var ErrNoParents = errors.New("given range has no parents")

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
