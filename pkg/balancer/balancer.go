package balancer

import (
	"fmt"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
)

type Balancer struct {
	ks   *ranje.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer
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

func (b *Balancer) operatorForce(id *ranje.Ident, node string) error {
	r, err := b.ks.GetByIdent(*id)
	if err != nil {
		return err
	}

	// TODO : Maybe reject this if the range is in some states (e.g. obsolete)

	// No validation here. The node might not exist yet, or have temporarily
	// gone away, or who knows what else.
	r.ForceNodeIdent = node

	return nil
}

// operatorSplit is called by the balancerServer when a controller.Split RPC is
// received. An operator wishes for this range to be split, for whatever reason.
func (b *Balancer) operatorSplit(r *ranje.Range, boundary ranje.Key, left, right string) error {
	if r.SplitRequest != nil {
		fmt.Printf("Warning: Replaced operator split\n")
	}

	r.Lock()
	defer r.Unlock()

	r.SplitRequest = &ranje.SplitRequest{
		Boundary:  boundary,
		NodeLeft:  left,
		NodeRight: right,
	}

	fmt.Printf("operator requested range %s split: %s\n", r.String(), r.SplitRequest)

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
		r.MustState(ranje.Placing)

		// TODO: Consider whether the range is being forced onto a specific node
		// here. could have happened before the initial placement.
		n := b.Candidate(r)

		// No candidates? That's a problem
		// TODO: Will result in quarantine eventually? Might not be range's fault
		if n == nil {
			r.MustState(ranje.PlaceError)
			continue
		}

		// Perform the placement in a background routine. When it terminates,
		// the range will be in the Ready or PlaceError states.
		go b.Place(r, n)
	}

	// Find any ranges in PlaceError and move them to Pending or Quarantine
	for _, r := range b.ks.RangesByState(ranje.PlaceError) {
		if r.NeedsQuarantine() {
			r.MustState(ranje.Quarantined)
			continue
		}
		r.MustState(ranje.Pending)
	}

	// Find any ranges which should be forced onto a specific node.
	for _, r := range b.ks.RangesForcing() {
		n := b.rost.NodeByIdent(r.ForceNodeIdent)

		// The ident didn't match any node? Operator probably made a mistake, so
		// leave it where it is.
		if n == nil {
			fmt.Printf("tried to force range to unknown node: %s\n", r.ForceNodeIdent)
			continue
		}

		// Clear this now that we've found the destination node, to avoid
		// confusion.
		// TODO: Lock the range! This is a mutation!
		r.ForceNodeIdent = ""

		if r.State() == ranje.Ready {
			r.MustState(ranje.Moving)
			go b.Move(r, r.MoveSrc(), n)

		} else if r.State() == ranje.Quarantined {
			r.MustState(ranje.Placing)
			go b.Place(r, n)

		} else {
			panic("force-placing pending ranges not implemented yet")
		}

	}
}

func (b *Balancer) Candidate(r *ranje.Range) *ranje.Node {
	b.rost.RLock()
	defer b.rost.RUnlock()

	var best *ranje.Node

	// lol
	for _, n := range b.rost.Nodes {
		best = n
		break
	}

	// No healthy nodes?
	if best == nil {
		fmt.Printf("no candidate nodes to place range: %s\n", r.String())
		return nil
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
func (b *Balancer) Move(r *ranje.Range, src *ranje.Placement, destNode *ranje.Node) {
	r.AssertState(ranje.Moving)

	// TODO: If src and dest are the same node (i.e. the range is moving to the same node, we get stuck in Taken)

	// TODO: Could use an extra step here to clear the move with the dest node first.

	dest, err := ranje.NewPlacement(r, destNode)
	if err != nil {
		//return nil, fmt.Errorf("couldn't Give range; error creating placement: %s", err)
		// TODO: Do something less dumb than this.
		r.MustState(ranje.Ready)
		return
	}

	// 1. Take
	err = src.Take()
	if err != nil {
		fmt.Printf("Take failed: %s\n", err.Error())
		r.MustState(ranje.Ready) // ???
		return
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

	// Drop
	err = src.Drop()
	if err != nil {
		fmt.Printf("Drop failed: %s\n", err.Error())
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	// Serve
	err = dest.Serve()
	if err != nil {
		fmt.Printf("Serve failed: %s\n", err.Error())
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return
	}

	src.Forget()

	r.MustState(ranje.Ready)
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.rebalance()
	}
}
