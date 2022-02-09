package ranje

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

// Range is a range of keys in the keyspace.
// These should probably only be instantiated by Keyspace? No sanity checks, so be careful.
type Range struct {
	pers Persister
	Meta Meta

	state    StateLocal
	parents  []*Range
	children []*Range

	// Which node currently has the range, and which it is moving to.
	// TODO: Each of these are probably only valid in some states. Doc that.
	// TODO: Placement fucks with these directly. Don't do that.
	curr *DurablePlacement
	next *DurablePlacement

	// The number of times this range has failed to be placed since it was last
	// Ready. Incremented by State.
	placeErrorCount int

	// Guards everything.
	sync.Mutex
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
	if r.curr != nil {
		f = fmt.Sprintf("%s (curr: %s)", f, r.curr.DumpForDebug())
	}
	if r.next != nil {
		f = fmt.Sprintf("%s (next: %s)", f, r.next.DumpForDebug())
	}
	fmt.Printf(" - %s%s\n", r.String(), f)
}

func (r *Range) Placement() *DurablePlacement {
	return r.curr
}

func (r *Range) NextPlacement() *DurablePlacement {
	return r.next
}

// TODO: Use Placement instead of this!
func (r *Range) MoveSrc() *DurablePlacement {

	// TODO: Only really need RLock here.
	r.Lock()
	defer r.Unlock()

	if r.curr == nil {
		return nil
	}

	// TODO: Is this necessary?
	if r.curr.state != SpReady {
		panic(fmt.Sprintf("movesrc called on range %s, where placement is not ready", r.String()))
	}

	return r.curr
}

// TODO: Does this actually need the `giving` param?
func (r *Range) GiveRequest(giving *DurablePlacement) (*pb.GiveRequest, error) {
	rm := r.Meta.ToProto()

	// Build a list of the other current placement of this exact range. This
	// doesn't include the ranges which this range was split/joined from! It'll
	// be empty the first time the range is being placed, and have one entry
	// during normal moves.
	parents := []*pb.Placement{}
	if p := r.curr; p != nil {

		// This indicates that the caller is very confused
		if p.state == SpPending && p == giving {
			panic("giving current placement??")
		}

		if p.state != SpTaken {
			return nil, fmt.Errorf("can't give range %s when current placement on node %s is in state %s",
				r.String(), p.nodeID, p.state)
		}

		parents = append(parents, &pb.Placement{
			Range: rm,
			Node:  p.NodeID(),
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

		// Include the node where the parent range can currently be found, if
		// it's still placed, such as during a split. Older ranges might not be.
		node := ""
		if p := rr.curr; p != nil {
			node = p.NodeID()
		}

		*parents = append(*parents, &pb.Placement{
			Range: rr.Meta.ToProto(),
			Node:  node,
		})

		// TODO: Recurse?
	}
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

	if old == new {
		fmt.Printf("%s %s -> %s REDUNDANT STATE CHANGE\n", r.String(), old, new)
		return nil
	}

	if new == Unknown {
		return errors.New("can't transition range into Unknown")
	}

	// Obsolete is terminal. The range should be destroyed.
	if old == Obsolete {
		return errors.New("can't transition range out of SpDropped")
	}

	ok := false

	if old == Pending && new == Placing { // 1
		ok = true
	}

	// Straight from Pending to PlaceError means that we can't even attempt a
	// placement. Probably no candidate nodes available. Currently increments
	// error count anyway, which might result in range quarantine!
	if old == Pending && new == PlaceError { // NEEDS NUM
		r.placeErrorCount += 1
		ok = true
	}

	// TODO: THIS IS ONLY INITIAL PLACEMENT
	if old == Placing && new == Ready { // 2
		r.placeErrorCount = 0
		ok = true
	}

	// Started placing the range, but it failed. Maybe that's because the range
	// is toxic. Or maybe just unlucky timing and the destination ndoe died.
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

	if old == Moving && new == Ready { // 8
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

	// Update durable storage first
	err := r.pers.PutState(r, new)
	if err != nil {
		r.state = old
		return fmt.Errorf("while persisting range state: %s", err)
	}

	r.state = new

	// Notify parent(s) of state change, so they can change their own state in
	// response.
	// TODO: Does this need to be transactional with the above state change? This won't be called again during rehydration.
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

func (r *Range) InitPersist() error {

	// Calling this method any time other than immediately after instantiating
	// is a bug. Every other transition should happen via ToState.
	if r.state != Pending {
		panic("InitPersist called on non-pending range")
	}

	return r.pers.Create(r)
}

func (r *Range) ParentIdents() []Ident {
	ids := make([]Ident, len(r.parents))
	for i, r := range r.parents {
		ids[i] = r.Meta.Ident
	}
	return ids
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

// Caller must NOT hold the range lock.
func (r *Range) ClearNextPlacement() {
	r.Lock()
	defer r.Unlock()

	if r.next == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	r.next = nil
}

// Caller must NOT hold the range lock.
func (r *Range) CompleteNextPlacement() error {
	r.Lock()
	defer r.Unlock()

	if r.next == nil {
		// This method should not even be called in this state!
		panic("can't complete move when next placement is nil")
	}

	// During inital placement, it's okay for there to be no current placement.
	// Otherwise notify the placement that we're about to destroy it. (This is
	// a gross hack, because Node also has a pointer to the placement for now.)
	// TODO: Should this be a totally separate path?
	if r.curr != nil {
		err := r.curr.Forget()
		if err != nil {
			return err
		}
	}

	r.curr = r.next
	r.next = nil

	return nil
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
