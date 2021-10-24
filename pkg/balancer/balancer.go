package balancer

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Balancer struct {
	ks   *ranje.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer

	moveReqs  []MoveRequest
	splitReqs []SplitRequest
	joinReqs  []JoinRequest
	reqsMu    sync.Mutex
}

func New(ks *ranje.Keyspace, rost *roster.Roster, srv *grpc.Server) *Balancer {
	b := &Balancer{
		ks:   ks,
		rost: rost,
		srv:  srv,
	}

	// Register the gRPC server to receive instructions from operators. This
	// will hopefully not be necessary once balancing actually works!
	b.bs = &balancerServer{bal: b}
	pb.RegisterBalancerServer(srv, b.bs)

	return b
}

func (b *Balancer) opMove(req MoveRequest) {
	b.reqsMu.Lock()
	defer b.reqsMu.Unlock()
	b.moveReqs = append(b.moveReqs, req)
	fmt.Printf("MoveRequest: %v\n", req)
}

// operatorSplit is called by the balancerServer when a controller.Split RPC is
// received. An operator wishes for this range to be split, for whatever reason.
// TODO: Just take the ident! Why do we need the whole range here?
// TODO: Actually why take the range at all? Just record the boundary.
func (b *Balancer) operatorSplit(r *ranje.Range, boundary ranje.Key, left, right string) error {
	b.reqsMu.Lock()
	defer b.reqsMu.Unlock()

	req := SplitRequest{
		Range:     r.Meta.Ident,
		Boundary:  boundary,
		NodeLeft:  left,
		NodeRight: right,
	}

	b.splitReqs = append(b.splitReqs, req)
	fmt.Printf("split request: %v\n", req)

	return nil
}

// TODO: Just take the ident! Why do we need the whole ranges here?
func (b *Balancer) operatorJoin(left, right *ranje.Range, node string) error {
	b.reqsMu.Lock()
	defer b.reqsMu.Unlock()

	req := JoinRequest{
		Left:  left.Meta.Ident,
		Right: right.Meta.Ident,
		Node:  node,
	}

	b.joinReqs = append(b.joinReqs, req)
	fmt.Printf("join request: %v\n", req)

	return nil
}

// This is only here to proxy Keyspace.GetByIdent.
func (b *Balancer) getRange(id ranje.Ident) (*ranje.Range, error) {
	return b.ks.GetByIdent(id)
}

func (b *Balancer) rangeCanBeSplit(r *ranje.Range) error {
	// TODO: This isn't right for all cases, only the kv example.
	// The kv example transfers data directly between nodes (just to be obtuse),
	// which they're only able to do when the range is ready. In production we'd
	// more likely be writing to a WAL in some durable store which could be read
	// from regardless of the state of the source node.
	// Need some kind of global config here to switch these behaviors.
	if r.State() != ranje.Ready {
		return fmt.Errorf("only Ready ranges can be split")
	}

	return nil
}

func (b *Balancer) rebalance() {
	fmt.Printf("rebalancing\n")

	// Find any unknown and complain about them. There should be NONE of these
	// in the keyspace; it indicates a state bug.
	for _, r := range b.ks.RangesByState(ranje.Unknown) {
		panic(fmt.Sprintf("range in unknown state: %s\n", r.String()))
		//fmt.Printf("range in unknown state: %s\n", r.String())
	}

	// Find any pending ranges and find any node to assign them to.
	for _, r := range b.ks.RangesByState(ranje.Pending) {
		//r.MustState(ranje.Placing)

		// Find a node to place this range on.
		nid := b.Candidate(r)

		// No candidates? That's a problem
		// TODO: Will result in quarantine? Might not be range's fault.
		if nid == "" {
			r.MustState(ranje.PlaceError)
			continue
		}

		// Perform the placement in a background goroutine. (It's just a special
		// case of moving with no source.) When it terminates, the range will be
		// in the Ready or PlaceError states.
		go b.Move(MoveRequest{
			Range: r.Meta.Ident,
			Node:  nid,
		})
	}

	// Find any ranges in PlaceError and move them to Pending or Quarantine
	for _, r := range b.ks.RangesByState(ranje.PlaceError) {
		if r.NeedsQuarantine() {
			r.MustState(ranje.Quarantined)
			continue
		}
		r.MustState(ranje.Pending)
	}

	// Moves

	b.reqsMu.Lock()
	moveReqs := b.moveReqs
	b.moveReqs = nil
	b.reqsMu.Unlock()

	if len(moveReqs) > 0 {
		for _, req := range moveReqs {
			go b.Move(req)
		}
	}

	// Splits

	b.reqsMu.Lock()
	splitReqs := b.splitReqs
	b.splitReqs = nil
	b.reqsMu.Unlock()

	if len(splitReqs) > 0 {
		for _, req := range splitReqs {
			go b.Split(req)
		}
	}

	// Joins

	// Can we skip the queue and call Join right from operatorJoin? Or Earlier?
	b.reqsMu.Lock()
	joinReqs := b.joinReqs
	b.joinReqs = nil
	b.reqsMu.Unlock()
	if len(joinReqs) > 0 {
		for _, req := range joinReqs {
			go b.Join(req)
		}
	}

}

func (b *Balancer) Candidate(r *ranje.Range) string {
	b.rost.RLock()
	defer b.rost.RUnlock()

	var best string

	// lol
	for nid := range b.rost.Nodes {
		best = nid
		break
	}

	// No suitable nodes?
	if best == "" {
		fmt.Printf("no candidate nodes to place range: %s\n", r.String())
		return ""
	}

	return best
}

func (b *Balancer) Place(r *ranje.Range, n *ranje.Node) {
	r.AssertState(ranje.Placing)

	dest, err := ranje.NewPlacement(r, n)
	if err != nil {
		r.MustState(ranje.PlaceError)
		return
	}

	err = dest.Give()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		r.MustState(ranje.PlaceError)
		return
	}

	r.MustState(ranje.Ready)
}

// TODO: Can this be combined with move? Maybe most steps just do nothing.
//func (b *Balancer) Move(r *ranje.Range, src *ranje.Placement, destNode *ranje.Node) {
func (b *Balancer) Move(req MoveRequest) {

	// TODO: Lock ks.ranges!
	r, err := b.ks.GetByIdent(req.Range)
	if err != nil {
		fmt.Printf("Move failed: %s\n", err.Error())
		return
	}

	node := b.rost.NodeByIdent(req.Node)
	if node == nil {
		fmt.Printf("Move failed: No such node: %s\n", req.Node)
		return
	}

	var src *ranje.Placement

	// If the range is currently ready, it's currently places on some node.
	if r.State() == ranje.Ready {
		fmt.Printf("Moving: %s\n", r)
		r.MustState(ranje.Moving)
		src = r.MoveSrc()

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		fmt.Printf("Placing: %s\n", r)
		r.MustState(ranje.Placing)

	} else {
		panic(fmt.Sprintf("unexpectd range state?! %s", r.State()))
	}

	// TODO: If src and dest are the same node (i.e. the range is moving to the same node, we get stuck in Taken)
	// TODO: Could use an extra step here to clear the move with the dest node first.

	dest, err := ranje.NewPlacement(r, node)
	if err != nil {
		//return nil, fmt.Errorf("couldn't Give range; error creating placement: %s", err)
		// TODO: Do something less dumb than this.
		r.MustState(ranje.Ready)
		return
	}

	// 1. Take
	// (only if moving; skip if doing initial placement)

	if src != nil {
		err = src.Take()
		if err != nil {
			fmt.Printf("Take failed: %s\n", err.Error())
			r.MustState(ranje.Ready) // ???
			return
		}
	}

	// 2. Give

	err = dest.Give()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	// Wait for the placement to become Ready (which it might already be).

	if src != nil {
		err = dest.FetchWait()
		if err != nil {
			// TODO: Provide a more useful error here
			fmt.Printf("Fetch failed: %s\n", err.Error())
			return
		}
	}

	// 3. Drop
	// (only if moving; skip if doing initial placement)

	if src != nil {
		err = src.Drop()
		if err != nil {
			fmt.Printf("Drop failed: %s\n", err.Error())
			// No state change. Stay in Moving.
			// TODO: Repair the situation somehow.
			//r.MustState(ranje.MoveError)
			return
		}
	}

	// 4. Serve

	if src != nil {
		err = dest.Serve()
		if err != nil {
			fmt.Printf("Serve failed: %s\n", err.Error())
			// No state change. Stay in Moving.
			// TODO: Repair the situation somehow.
			//r.MustState(ranje.MoveError)
			return
		}
	}

	r.MustState(ranje.Ready)

	// 5. Cleanup

	if src != nil {
		src.Forget()
	}
}

func (b *Balancer) Join(req JoinRequest) {

	// TODO: Lock ks.ranges!
	r1, err := b.ks.GetByIdent(req.Left)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	// TODO: Lock ks.ranges!
	r2, err := b.ks.GetByIdent(req.Right)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	p1 := r1.MoveSrc()
	p2 := r2.MoveSrc()

	node := b.rost.NodeByIdent(req.Node)
	if node == nil {
		fmt.Printf("Join failed: No such node: %s\n", req.Node)
		return
	}

	// Moves r1 and r2 into Joining state.
	// Starts dest in Pending state. (Like all ranges!)
	// Returns error if either of the ranges aren't ready, or if they're not adjacent.
	r3, err := b.ks.JoinTwo(r1, r2)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	fmt.Printf("Joining: %s, %s -> %s\n", r1, r2, r3)

	// 1. Take

	// TODO: Pass the context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error { return p1.Take() })
	g.Go(func() error { return p2.Take() })
	err = g.Wait()
	if err != nil {
		fmt.Printf("Join (Take) failed: %s\n", err.Error())
		return
	}

	// 2. Give

	r3.MustState(ranje.Placing)

	p3, err := ranje.NewPlacement(r3, node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	err = p3.Give()
	if err != nil {
		fmt.Printf("Join (Give) failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	// Wait for the placement to become Ready (which it might already be).
	err = p3.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Join (Fetch) failed: %s\n", err.Error())
		return
	}

	// 3. Drop

	g, _ = errgroup.WithContext(context.Background())
	g.Go(func() error { return p1.Drop() })
	g.Go(func() error { return p2.Drop() })
	err = g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Join (Drop) failed: %s\n", err.Error())
		return
	}

	// 4. Serve

	err = p3.Serve()
	if err != nil {
		fmt.Printf("Join (Serve) failed: %s\n", err.Error())
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	r3.MustState(ranje.Ready)

	// 5. Cleanup

	for _, p := range []*ranje.Placement{p1, p2} {
		p.Forget()
	}

	for _, r := range []*ranje.Range{r1, r2} {
		r.MustState(ranje.Obsolete)

		// TODO: This part should probably be handled later by some kind of GC.
		err = b.ks.Discard(r)
		if err != nil {
			fmt.Printf("Join (Discard) failed: %s\n", err.Error())
		}
	}
}

func (b *Balancer) Split(req SplitRequest) {

	nLeft := b.rost.NodeByIdent(req.NodeLeft)
	if nLeft == nil {
		fmt.Printf("Split failed: No such node (left): %s\n", req.NodeLeft)
		return
	}

	nRight := b.rost.NodeByIdent(req.NodeRight)
	if nRight == nil {
		fmt.Printf("Split failed: No such node (right): %s\n", req.NodeRight)
		return
	}

	// TODO: Lock ks.ranges!
	r, err := b.ks.GetByIdent(req.Range)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	// TODO: Remove this, use prev/curr
	src := r.MoveSrc()

	// Moves r into Splitting state
	// TODO: Rename MoveSrc! Clearly it's not just that.
	err = b.ks.DoSplit(r, req.Boundary)
	if err != nil {
		fmt.Printf("DoSplit failed: %s\n", err.Error())
		return
	}

	// Only exactly two sides of the split for now
	rLeft, err := r.Child(0)
	if err != nil {
		fmt.Printf("DoSplit failed, getting left child: %s\n", err.Error())
		return
	}
	rRight, err := r.Child(1)
	if err != nil {
		fmt.Printf("DoSplit failed, getting right child: %s\n", err.Error())
		return
	}

	rLeft.MustState(ranje.Placing)
	pLeft, err := ranje.NewPlacement(rLeft, nLeft)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	rRight.MustState(ranje.Placing)
	pRight, err := ranje.NewPlacement(rRight, nRight)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return
	}

	// 1. Take

	err = src.Take()
	if err != nil {
		fmt.Printf("Take failed: %s\n", err.Error())
		return
	}

	// 2. Give

	// TODO: Pass a context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	for side, p := range map[string]*ranje.Placement{"left": pLeft, "right": pRight} {

		// Keep hold of current values for closure.
		// https://golang.org/doc/faq#closures_and_goroutines
		side := side
		p := p

		g.Go(func() error {

			// TODO: This doesn't work yet! Give doesn't include parents info.
			err = p.Give()
			if err != nil {
				return fmt.Errorf("give (%s) failed: %s", side, err.Error())
			}

			// Wait for the placement to become Ready (which it might already be).
			err = p.FetchWait()
			if err != nil {
				// TODO: Provide a more useful error here
				return fmt.Errorf("fetch (%s) failed: %s", side, err.Error())
			}

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		return
	}

	// 3. Drop

	err = src.Drop()
	if err != nil {
		fmt.Printf("Drop failed: %s\n", err.Error())
		return
	}

	// 4. Serve

	g, _ = errgroup.WithContext(context.Background())
	g.Go(func() error { return pLeft.Serve() })
	g.Go(func() error { return pRight.Serve() })
	err = g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Serve (Drop) failed: %s\n", err.Error())
		return
	}

	rLeft.MustState(ranje.Ready)
	rRight.MustState(ranje.Ready)

	src.Forget()

	// Redundant?
	r.MustState(ranje.Obsolete)

	err = b.ks.Discard(r)
	if err != nil {
		fmt.Printf("Discard failed: %s\n", err.Error())
	}
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.rebalance()
	}
}
