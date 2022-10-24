package keyspace

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
)

type RangeFamily struct {
	Range    *ranje.Range
	Siblings []*ranje.Range
	Parents  []*ranje.Range
	Children []*ranje.Range
}

// Family returns the range family (parents, siblings, children) of the given
// range. This is useful when determining whether the range should transition.
func (ks *Keyspace) Family(rID ranje.Ident) (*RangeFamily, error) {
	f := &RangeFamily{}

	r, err := ks.Get(rID)
	if err != nil {
		return nil, err
	}
	f.Range = r

	s, err := siblings(ks, r)
	if err != nil {
		return nil, err
	}
	f.Siblings = s

	p, err := parents(ks, r)
	if err != nil {
		return nil, err
	}
	f.Parents = p

	c, err := children(ks, r)
	if err != nil {
		return nil, err
	}
	f.Children = c

	return f, nil
}

// TODO: These helpers are all private now, refactor them.

// geneses returns all of the ranges which have no parents. Usually there'll be
// exactly one, but that isn't an invariant.
func geneses(ks *Keyspace) []*ranje.Range {
	out := []*ranje.Range{}

	for _, r := range ks.ranges {
		if len(r.Parents) == 0 {
			out = append(out, r)
		}
	}

	return out
}

func children(ks *Keyspace, r *ranje.Range) ([]*ranje.Range, error) {
	children := make([]*ranje.Range, len(r.Children))

	for i, rID := range r.Children {
		rp, err := ks.Get(rID)
		if err != nil {
			return nil, fmt.Errorf("range has invalid child: %s (r=%s)", rID, r)
		}

		children[i] = rp
	}

	return children, nil
}

// parents returns the parents of the given range, i.e. those it was split or
// joined from. Child ranges should probably mostly ignore their parents.
func parents(ks *Keyspace, r *ranje.Range) ([]*ranje.Range, error) {
	parents := make([]*ranje.Range, len(r.Parents))

	for i, rID := range r.Parents {
		rp, err := ks.Get(rID)
		if err != nil {
			return nil, fmt.Errorf("range has invalid parent: %s", rID)
		}

		parents[i] = rp
	}

	return parents, nil
}

func siblings(ks *Keyspace, r *ranje.Range) ([]*ranje.Range, error) {
	if len(r.Parents) == 0 {
		// Special case: If the range has no parents, then it is a genesis
		// range, i.e. it was spawned when the keyspace was created, not by
		// splitting or joining any other range.
		siblings := make([]*ranje.Range, 0)
		for _, rr := range geneses(ks) {
			if rr != r {
				siblings = append(siblings, rr)
			}
		}
		return siblings, nil

	} else if len(r.Parents) > 1 {
		// If the range has more than one parent, it was produced by a join
		// operation, and thus has no siblings. Only ranges produced by splits
		// have siblings.
		return []*ranje.Range{}, nil
	}

	// Otherwise, this range has one parent, so was produced via a split. Find
	// siblings via that parent.

	rp, err := ks.Get(r.Parents[0])
	if err != nil {
		return nil, fmt.Errorf("CORRUPT: range has invalid parent: %s", r.Parents[0])
	}

	siblings := make([]*ranje.Range, 0)
	for _, rID := range rp.Children {

		// Exclude self.
		if rID == r.Meta.Ident {
			continue
		}

		rs, err := ks.Get(rID)
		if err != nil {
			return nil, fmt.Errorf("CORRUPT: range has invalid parent: %s", rID)
		}

		siblings = append(siblings, rs)
	}

	return siblings, nil
}
