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

func (b *Balancer) Force(id *ranje.Ident, node string) error {
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

		r.MustState(ranje.Placing)
		go b.Place(r, n)
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

	err := n.Give(r.Meta.Ident, r)
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		r.MustState(ranje.PlaceError)
		return
	}

	r.MustState(ranje.Ready)
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.rebalance()
	}
}
