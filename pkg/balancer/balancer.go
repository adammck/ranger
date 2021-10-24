package balancer

import (
	"fmt"
	"sync"
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

	ops   []OpRunner
	opsMu sync.RWMutex
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

func (b *Balancer) Operation(req OpRunner) {
	b.opsMu.Lock()
	defer b.opsMu.Unlock()
	b.ops = append(b.ops, req)
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
		req := MoveRequest{
			Range: r.Meta.Ident,
			Node:  nid,
		}
		go req.Run(b)
	}

	// Find any ranges in PlaceError and move them to Pending or Quarantine
	for _, r := range b.ks.RangesByState(ranje.PlaceError) {
		if r.NeedsQuarantine() {
			r.MustState(ranje.Quarantined)
			continue
		}
		r.MustState(ranje.Pending)
	}

	// Kick off any pending operator-initiated actions in goroutines.

	b.opsMu.RLock()
	for _, req := range b.ops {
		go req.Run(b)
	}
	b.opsMu.RUnlock()
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

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.rebalance()
	}
}
