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
	curr *Placement
	next *Placement

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

// TODO: This method is pointless now that we have curr/next; remove it.
func (r *Range) MoveSrc() *Placement {

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

func (r *Range) GiveRequest(giving *Placement) (*pb.GiveRequest, error) {
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

		// Include the node where the parent range can currently be found, if
		// it's still placed, such as during a split. Older ranges might not be.
		node := ""
		if p := rr.curr; p != nil {
			node = p.Addr()
		}

		*parents = append(*parents, &pb.Placement{
			Range: rr.Meta.ToProto(),
			Node:  node,
		})

		// TODO: Recurse?
	}
}

// Caller must hold the lock for writing.
func (r *Range) UnsafeForgetPlacement(p *Placement) error {
	// TODO: Surely this is only needed in one of these cases?

	if r.curr == p {
		r.curr = nil
		return nil
	}

	if r.next == p {
		r.next = nil
		return nil
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

	if old == Placing && new == Ready { // 2
		// TODO: Is this where we promote next to curr?
		if r.curr != nil {
			return errors.New("can't transition from Placing to Ready when r.curr is not nil")
		}
		if r.next == nil {
			return errors.New("can't transition from Placing to Ready when r.next is nil")
		}

		// Promote next range to current.
		r.curr = r.next
		r.next = nil

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

func (r *Range) PlacementStateChanged(p *Placement) {

	// This will deadlock if we call ToState!
	// TODO: Must be holding range lock to call?
	//r.Lock()
	//defer r.Unlock()

	switch r.state {
	case Pending:
		r.assertNoCurr()

		if p == r.next {
			// TODO: Is SpPending right here?
			if r.next.state == SpPending || r.next.state == SpFetching || r.next.state == SpFetched {
				r.MustState(Placing)
				return
			}
		}

	case Placing:
		r.assertNoCurr()

		// This indicates a bug
		if r.next == nil {
			panic(fmt.Sprintf("placing range %s has nil next placement", r.String()))
		}

		if p == r.next {
			if r.next.state == SpFetchFailed {
				// TODO: Is this where we choose PlaceError or Quarantine?
				r.MustState(PlaceError)
				return
			}
			if r.next.state == SpReady {
				r.MustState(Ready) // Promotes next to curr
				return
			}
		}

	case PlaceError:
		r.assertNoCurr()
		if p == r.next {
			if r.next.state == SpPending {
				if r.NeedsQuarantine() {
					r.MustState(Quarantined)
				} else {
					r.MustState(Pending)
				}
			}
		}
	}
}

func (r *Range) assertNoCurr() {
	if r.curr != nil {
		panic(fmt.Sprintf("pending range %s has non-nil curr placement", r.String()))
	}
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
