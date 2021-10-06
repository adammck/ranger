package keyspace

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/keyspace/fsm"
)

// Keyspace is an overlapping set of ranges which cover all of the possible
// values in the space. Any value is covered by either one or two ranges: one
// range in the steady-state, where nothing is being moved around, and two
// ranges while rebalancing is in progress.
type Keyspace struct {
	ranges    []*Range // TODO: don't be dumb, use an interval tree
	mu        sync.Mutex
	nextIdent int
}

func New() *Keyspace {
	ks := &Keyspace{}
	ks.ranges = []*Range{ks.Range()}
	return ks
}

func NewWithSplits(splits []string) *Keyspace {
	ks := &Keyspace{}
	rs := make([]*Range, len(splits)+1)

	// TODO: Should we sort the splits here? Or panic? We currently assume they're sorted.

	for i := range rs {
		var s, e Key

		if i > 0 {
			s = rs[i-1].end
		} else {
			s = ZeroKey
		}

		if i < len(splits) {
			e = Key(splits[i])
		} else {
			e = ZeroKey
		}

		r := ks.Range()
		r.start = s
		r.end = e
		rs[i] = r
	}

	ks.ranges = rs
	return ks
}

// Range returns a new range with the next available ident. This is the only
// way that a Range should be constructed.
func (ks *Keyspace) Range() *Range {
	r := &Range{
		ident: ks.nextIdent,
	}

	ks.nextIdent += 1

	return r
}

func (ks *Keyspace) Dump() string {
	s := make([]string, len(ks.ranges))

	for i, r := range ks.ranges {
		s[i] = r.String()
	}

	return strings.Join(s, " ")
}

// Get returns a range by its index.
func (ks *Keyspace) Get(ident int) *Range {
	for _, r := range ks.ranges {
		if r.ident == ident {
			return r
		}
	}

	// TODO: Make this an error, lol
	panic("no such ident")
}

// Len returns the number of ranges.
// This is mostly for testing, maybe remove it.
func (ks *Keyspace) Len() int {
	return len(ks.ranges)
}

// TODO: Rename to Split once the old one is gone
func (ks *Keyspace) DoSplit(r *Range, k Key) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if k == ZeroKey {
		return fmt.Errorf("can't split on zero key")
	}

	if r.state != fsm.Ready {
		return errors.New("can't split non-ready range")
	}

	// This should not be possible. Panic?
	if len(r.children) > 0 {
		return fmt.Errorf("range %s already has %d children", r, len(r.children))
	}

	if !r.Contains(k) {
		return fmt.Errorf("range %s does not contain key: %s", r, k)
	}

	if k == r.start {
		return fmt.Errorf("range %s starts with key: %s", r, k)
	}

	err := r.State(fsm.Splitting)
	if err != nil {
		// The error is clear enough, no need to wrap it.
		return err
	}

	// TODO: Move this part into Range?

	one := ks.Range()
	one.start = r.start
	one.end = k
	one.parents = []*Range{r}

	two := ks.Range()
	two.start = k
	two.end = r.end
	two.parents = []*Range{r}

	// append to the end of the ranges
	// TODO: Insert the children after the parent, not at the end!
	ks.ranges = append(ks.ranges, one)
	ks.ranges = append(ks.ranges, two)

	r.children = []*Range{one, two}

	return nil
}

func (ks *Keyspace) JoinTwo(one *Range, two *Range) (*Range, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	for _, r := range []*Range{one, two} {
		if r.state != fsm.Ready {
			return nil, errors.New("can't join non-ready ranges")
		}

		// This should not be possible. Panic?
		if len(r.children) > 0 {
			return nil, fmt.Errorf("range %s already has %d children", r, len(r.children))
		}
	}

	if one.end != two.start {
		return nil, fmt.Errorf("not adjacent: %s, %s", one, two)
	}

	for _, r := range []*Range{one, two} {
		err := r.State(fsm.Joining)
		if err != nil {
			// The error is clear enough, no need to wrap it.
			return nil, err
		}
	}

	// TODO: Move this part into Range?

	three := ks.Range()
	three.start = one.start
	three.end = two.end
	three.parents = []*Range{one, two}

	// Insert new range at the end.
	ks.ranges = append(ks.ranges, three)

	one.children = []*Range{three}
	two.children = []*Range{three}

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

	if r.state != fsm.Obsolete {
		return errors.New("can't discard non-obsolete range")
	}

	// TODO: Is this necessary? Ranges are generally discarded after split/join, but so what?
	if len(r.children) == 0 {
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
