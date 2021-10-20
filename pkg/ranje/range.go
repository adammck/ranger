package ranje

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// TODO: Move this somewhere else!
type SplitRequest struct {
	Boundary  Key
	NodeLeft  string
	NodeRight string
}

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	Meta Meta

	state    StateLocal
	parents  []*Range
	children []*Range

	// Which nodes currently have this range, and what state are they in? May be
	// empty or have n entries, depending on the local state of the range.
	// TODO: Might be safer to replace this with a [2], or (before, after) pair.
	placements []*Placement

	// The number of times this range has failed to be placed since it was last
	// Ready. Incremented by State.
	placeErrorCount int

	// Hints
	// Public so the balancer can mess with them.
	// TODO: Should that happen via accessors instead?
	ForceNodeIdent string
	SplitRequest   *SplitRequest

	// Guards everything.
	sync.Mutex
}

// Contains returns true if the given key is within the range.
// TODO: Test this.
// TODO: Move this to Meta?
func (r *Range) Contains(k Key) bool {
	if r.Meta.Start != ZeroKey {
		if k < r.Meta.Start {
			return false
		}
	}

	if r.Meta.End != ZeroKey {
		// Note that the range end is exclusive!
		if k >= r.Meta.End {
			return false
		}
	}

	return true
}

func (r *Range) Child(index int) (*Range, error) {
	// TODO: Locking!

	if index < 0 {
		return nil, errors.New("negative index")
	}
	if index > len(r.children)-1 {
		return nil, errors.New("invalid index")
	}

	return r.children[index], nil
}

func (r *Range) AssertState(s StateLocal) {
	if r.state != s {
		panic(fmt.Sprintf("range failed state assertion %s != %s %s", r.state.String(), s.String(), r))
	}
}

func (r *Range) SameMeta(id Ident, start, end []byte) bool {
	// TODO: This method is batshit
	return r.Meta.Ident.Key == id.Key && r.Meta.Start == Key(start) && r.Meta.End == Key(end)
}

func (r *Range) String() string {
	return fmt.Sprintf("R{%s %s}", r.Meta.String(), r.state)
}

// TODO: Replace this with a statusz-type page
func (r *Range) DumpForDebug() {
	f := ""
	if r.ForceNodeIdent != "" {
		f = fmt.Sprintf("%s (forcing to: %s)", f, r.ForceNodeIdent)
	}
	if r.SplitRequest != nil {
		f = fmt.Sprintf("%s (splitting: %s)", f, r.SplitRequest)
	}
	fmt.Printf(" - %s%s\n", r.String(), f)
}

func (r *Range) MoveSrc() *Placement {

	// This indicates a bug.
	if len(r.placements) == 0 {
		panic(fmt.Sprintf("movesrc called on range %s, with 0 placements", r.String()))
	}

	if len(r.placements) > 1 {
		// Oh god something is really fucked up
		panic(fmt.Sprintf("movesrc called on range %s, with > 1 placements", r.String()))
	}

	// TODO: Only really need RLock here.
	r.Lock()
	defer r.Unlock()

	p := r.placements[0]

	if p.state != SpReady {
		panic(fmt.Sprintf("movesrc called on range %s, where placement is not ready", r.String()))
	}

	return p
}

func (r *Range) GiveRequest(giving *Placement) (*pb.GiveRequest, error) {
	rm := r.Meta.ToProto()

	// Build a list of the other current placements of this exact range. This
	// doesn't include the ranges which this range was split/joined from! It'll
	// be empty the first time the range is being placed, and have one entry
	// during normal moves.
	parents := []*pb.Placement{}
	for _, p := range r.placements {

		// Ignore the node that we're giving, which is in pending.
		// TODO: This is ugly enough that it probably doesn't belong here...
		if p.state == SpPending && p == giving {
			continue
		}

		if p.state != SpTaken {
			return nil, fmt.Errorf("can't give range %s when state of placement on node %s is %s",
				r.String(), p.node.String(), p.state)
		}

		parents = append(parents, &pb.Placement{
			Range: rm,
			Node:  p.Addr(),
		})
	}

	// Add one generations of parents
	// That's all we support right now
	addParents(r, &parents)

	return &pb.GiveRequest{
		Range:   rm,
		Parents: parents,
	}, nil
}

func addParents(r *Range, parents *[]*pb.Placement) {
	for _, rr := range r.parents {

		node := ""
		if len(rr.placements) > 0 {
			node = rr.placements[0].Addr()
		}

		*parents = append(*parents, &pb.Placement{
			Range: rr.Meta.ToProto(),
			Node:  node,
		})

		// TODO: Recurse?
	}
}

func (r *Range) UnsafeForgetPlacement(p *Placement) error {
	for i, p_ := range r.placements {
		if p == p_ {

			// argh, golang, why are you like this
			// https://github.com/golang/go/wiki/SliceTricks
			r.placements[i] = r.placements[len(r.placements)-1]
			r.placements[len(r.placements)-1] = nil
			r.placements = r.placements[:len(r.placements)-1]

			return nil
		}
	}

	return fmt.Errorf("couldn't forget placement on node %s of range %s; not found",
		p.node.String(), r.String())
}

// MustState attempts to change the state of the range to s, and panics if the
// transition is invalid. Callers should only ever attempt valid state changes
// anyway, but...
func (r *Range) MustState(s StateLocal) {
	err := r.ToState(s)
	if err != nil {
		panic(fmt.Sprintf("MustState: %s", err.Error()))
	}
}

func (r *Range) State() StateLocal {
	return r.state
}

// ToState change the state of the range to s or returns an error.
func (r *Range) ToState(new StateLocal) error {
	r.Lock()
	defer r.Unlock()
	old := r.state
	ok := false

	if new == Unknown {
		return errors.New("can't transition range into Unknown")
	}

	// Obsolete is terminal. The range should be destroyed.
	if old == Obsolete {
		return errors.New("can't transition range out of SpDropped")
	}

	if old == Pending && new == Placing { // 1
		ok = true
	}

	if old == Placing && new == Ready { // 2
		r.placeErrorCount = 0
		ok = true
	}

	if old == Placing && new == PlaceError { // 3
		r.placeErrorCount += 1
		ok = true
	}

	// WRONG
	// NOT (4)
	// THIS SHOULD GO BACK TO PLACING
	if old == PlaceError && new == Pending {
		ok = true
	}

	if old == PlaceError && new == Quarantined { // 5
		ok = true
	}

	// Doesn't happen automatically. Only when forced by an operator.
	if old == Quarantined && new == Placing { // 6
		ok = true
	}

	// SHOULD THIS GO BACK TO PENDING?
	if old == Ready && new == Placing {
		ok = true
	}

	if old == Ready && new == Moving { // 7
		ok = true
	}

	if old == Moving && new == Ready { // 7
		ok = true
	}

	if old == Ready && new == Splitting { // 9
		ok = true
	}

	if old == Ready && new == Joining { // 10
		ok = true
	}

	// This transition should only happen via CheckState.
	if (old == Splitting || old == Joining) && new == Obsolete {
		if !r.childrenReady() {
			return fmt.Errorf("invalid state transition: %s -> %s; children not ready", old, new)
		}

		ok = true
	}

	if !ok {
		return fmt.Errorf("invalid range state transition: %s -> %s", old, new)
	}

	r.state = new

	// Notify parent(s) of state change, so they can change their own state in
	// response.
	for _, parent := range r.parents {
		err := parent.ChildStateChanged()
		if err != nil {
			r.state = old
			return err
		}
	}

	fmt.Printf("%s %s -> %s\n", r.String(), old, new)
	return nil
}

func (r *Range) ChildStateChanged() error {

	// Splitting and joining ranges become obsolete once their children become ready.
	if r.state == Splitting || r.state == Joining {
		if r.childrenReady() {
			return r.ToState(Obsolete)
		}
	}

	return nil
}

func (r *Range) PlacementStateChanged(p *Placement) {

}

// childrenReady returns true if all of the ranges children are ready. (Doesn't
// care if the range has no children.)
func (r *Range) childrenReady() bool {
	for _, rr := range r.children {
		if rr.state != Ready {
			return false
		}
	}

	return true
}

func (r *Range) NeedsQuarantine() bool {
	return r.placeErrorCount >= 3
}
