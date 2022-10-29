package keyspace

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
)

// Operations returns the operations (splits, joins) which are currently in
// progress. Invalidated after any kind of transformation.
func (ks *Keyspace) Operations() ([]Operation, error) {

	// Build a set of active ranges to consider. This is churning through the
	// whole range history, but only really needs the leaf ranges. Keep an index
	// of those in the keyspace.
	ranges := map[ranje.Ident]*ranje.Range{}
	for _, r := range ks.ranges {
		if r.State == ranje.RsActive {
			ranges[r.Meta.Ident] = r
		}
	}

	ops := []Operation{}
	for _, r := range ranges {
		// Construct the operation from this range, however it's connected.
		op, err := opFromRange(ks, r)
		if err != nil {
			return nil, fmt.Errorf("finding operations: %w", err)
		}
		ops = append(ops, op)

		// Remove all of the ranges which are part of this operation from the
		// list, so we don't waste time constructing duplicate operations.
		for _, rID := range rangesFromOp(op) {
			delete(ranges, rID)
		}
	}

	return ops, nil
}

type Operation interface {
	Parents() []*ranje.Range
	Children() []*ranje.Range
}

type SplitOp struct {
	parent   *ranje.Range
	children []*ranje.Range
}

func (s *SplitOp) Parents() []*ranje.Range {
	return []*ranje.Range{s.parent}
}

func (s *SplitOp) Children() []*ranje.Range {
	return s.children
}

type JoinOp struct {
	parents []*ranje.Range
	child   *ranje.Range
}

func (j *JoinOp) Parents() []*ranje.Range {
	return j.parents
}

func (j *JoinOp) Children() []*ranje.Range {
	return []*ranje.Range{j.child}
}

func rangesFromOp(op Operation) []ranje.Ident {
	p := op.Parents()
	c := op.Children()
	out := make([]ranje.Ident, len(p)+len(c))

	for i, r := range p {
		out[i] = r.Meta.Ident
	}

	for i, r := range c {
		out[len(p)+i] = r.Meta.Ident
	}

	return out
}

func splitOpFromParent(ks *Keyspace, rID ranje.Ident) (Operation, error) {
	r, err := ks.Get(rID)
	if err != nil {
		return nil, err
	}

	children := make([]*ranje.Range, len(r.Children))
	for i := range r.Children {
		children[i], err = ks.Get(r.Children[i])
		if err != nil {
			return nil, err
		}
	}

	return &SplitOp{parent: r, children: children}, nil
}

func joinOpFromChild(ks *Keyspace, r *ranje.Range) (Operation, error) {
	parents := make([]*ranje.Range, len(r.Parents))
	var err error
	for i := range r.Parents {
		parents[i], err = ks.Get(r.Parents[i])
		if err != nil {
			return nil, err
		}
	}

	return &JoinOp{parents: parents, child: r}, nil
}

func opFromRange(ks *Keyspace, r *ranje.Range) (op Operation, err error) {
	if r.State != ranje.RsActive {
		panic("bug: called opFromRange with non-active range")
	}

	pl := len(r.Parents)
	if pl == 1 {
		return splitOpFromParent(ks, r.Parents[0])
	} else if pl == 2 {
		return joinOpFromChild(ks, r)
	}

	return nil, fmt.Errorf("can't infer operation type for rID=%d, given len(parents)=%d", r.Meta.Ident, pl)

}
