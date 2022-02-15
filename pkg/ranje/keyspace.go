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
	pers      Persister
	ranges    []*Range // TODO: don't be dumb, use an interval tree
	mu        sync.RWMutex
	nextIdent uint64
}

func New(persister Persister) *Keyspace {
	ks := &Keyspace{
		pers:      persister,
		nextIdent: 1,
	}

	// Start with one range that covers all keys.
	r := ks.Range()
	r.state = Pending
	r.InitPersist()

	ks.ranges = []*Range{r}
	return ks
}

// NewWithSplits is just for testing.
// TODO: Move this to the tests, why is it here?
func NewWithSplits(splits []string) *Keyspace {
	ks := &Keyspace{
		pers: nil, // TODO: Not ideal.
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
	r := &Range{
		pers:  ks.pers,
		state: Pending,
		Meta: Meta{
			Ident: Ident{
				Scope: "", // ???
				Key:   ks.nextIdent,
			},
		},
	}

	ks.nextIdent += 1

	return r
}

func (ks *Keyspace) RangesByState(s StateLocal) []*Range {
	out := []*Range{}

	for _, r := range ks.ranges {
		if r.state == s {
			out = append(out, r)
		}
	}

	return out
}

func (ks *Keyspace) Dump() string {
	s := make([]string, len(ks.ranges))

	for i, r := range ks.ranges {
		s[i] = r.String()
	}

	return strings.Join(s, " ")
}

// Get returns a range by its ident, or an error if no such range exists.
// TODO: Allow getting by other things.
// TODO: Should this lock ranges? Or the caller do it?
func (ks *Keyspace) GetByIdent(id Ident) (*Range, error) {
	for _, r := range ks.ranges {
		if r.Meta.Ident == id {
			return r, nil
		}
	}

	return nil, fmt.Errorf("no such range: %s", id.String())
}

// Get returns a range by its index.
// TODO: WTF is this method? Remove it!
func (ks *Keyspace) Get(ident int) *Range {
	for _, r := range ks.ranges {
		if int(r.Meta.Ident.Key) == ident {
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

	if r.state != Ready {
		return errors.New("can't split non-ready range")
	}

	// This should not be possible. Panic?
	if len(r.children) > 0 {
		return fmt.Errorf("range %s already has %d children", r, len(r.children))
	}

	if !r.Meta.Contains(k) {
		return fmt.Errorf("range %s does not contain key: %s", r, k)
	}

	if k == r.Meta.Start {
		return fmt.Errorf("range %s starts with key: %s", r, k)
	}

	err := r.ToState(Splitting)
	if err != nil {
		// The error is clear enough, no need to wrap it.
		return err
	}

	// TODO: Move this part into Range?

	one := ks.Range()
	one.Meta.Start = r.Meta.Start
	one.Meta.End = k
	one.parents = []*Range{r}
	if err := one.InitPersist(); err != nil {
		panic(fmt.Sprintf("failed to persist range: %s", err))
	}

	two := ks.Range()
	two.Meta.Start = k
	two.Meta.End = r.Meta.End
	two.parents = []*Range{r}
	if err := two.InitPersist(); err != nil {
		panic(fmt.Sprintf("failed to persist range: %s", err))
	}

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
		if r.state != Ready {
			return nil, errors.New("can't join non-ready ranges")
		}

		// This should not be possible. Panic?
		if len(r.children) > 0 {
			return nil, fmt.Errorf("range %s already has %d children", r, len(r.children))
		}
	}

	if one.Meta.End != two.Meta.Start {
		return nil, fmt.Errorf("not adjacent: %s, %s", one, two)
	}

	for _, r := range []*Range{one, two} {
		err := r.ToState(Joining)
		if err != nil {
			// The error is clear enough, no need to wrap it.
			return nil, err
		}
	}

	// TODO: Move this part into Range?

	three := ks.Range()
	three.Meta.Start = one.Meta.Start
	three.Meta.End = two.Meta.End
	three.parents = []*Range{one, two}
	if err := three.InitPersist(); err != nil {
		panic(fmt.Sprintf("failed to persist range: %s", err))
	}

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

	log.Printf("discarding: %s", r.String())

	if r.state != Obsolete {
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
