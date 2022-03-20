package balancer

import (
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/operations"
	"github.com/adammck/ranger/pkg/operations/move"
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
	dbg  *debugServer

	ops   []operations.Operation
	opsMu sync.RWMutex
	opsWG sync.WaitGroup
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

	// Register the debug server, to fetch info about the state of the world.
	// One could arguably pluck this straight from Consul -- since it's totally
	// consistent *right?* -- but it's a much richer interface to do it here.
	b.dbg = &debugServer{bal: b}
	pb.RegisterDebugServer(srv, b.dbg)

	return b
}

// Operations must be scheduled via this method, rather than invoked directly,
// to avoid races. Only the rebalance loop actually runs them.
func (b *Balancer) Operation(req operations.Operation) {
	b.opsMu.Lock()
	defer b.opsMu.Unlock()
	b.ops = append(b.ops, req)
}

func (b *Balancer) RangesOnNodesWantingDrain() []*ranje.Range {
	out := []*ranje.Range{}

	// TODO: Have the roster keep a list of nodes wanting drain rather than iterating.
	for _, n := range b.rost.Nodes {
		if n.WantDrain() {
			for _, pbnid := range b.ks.PlacementsByNodeID(n.Ident()) {

				// TODO: If the placement is next (i.e. currently moving onto this node), cancel the op.
				if pbnid.Position == 0 {
					out = append(out, pbnid.Range)
				}
			}
		}
	}

	return out
}

func (b *Balancer) Tick() {
	// Find any unknown and complain about them. There should be NONE of these
	// in the keyspace; it indicates a state bug.
	for _, r := range b.ks.RangesByState(ranje.Unknown) {
		log.Fatalf("range in unknown state: %v", r)
	}

	// Find any placements on nodes which are unknown to the roster. This can
	// happen if a node goes away while the controller is down, and probably
	// other state snafus.
	for _, pbnid := range b.ks.RangesOnNonExistentNodes(b.rost) {
		b.ks.PlacementToState(pbnid.Placement, ranje.SpGone)
	}

	// Find any pending ranges and find any node to assign them to.
	for _, r := range b.ks.RangesByState(ranje.Pending) {
		b.PerformMove(r)
	}

	// Find any ranges on nodes wanting drain, and move them.
	for _, r := range b.RangesOnNodesWantingDrain() {
		b.PerformMove(r)
	}

	// Kick off any pending operator-initiated actions in goroutines.

	b.opsMu.RLock()
	ops := b.ops
	b.ops = nil
	for _, req := range ops {
		err := operations.Run(req, &b.opsWG)
		if err != nil {
			log.Printf("Error initiating operation: %v", err)
		}
	}
	b.opsMu.RUnlock()
}

// FinishOps blocks until all operations have finished. It does nothing to
// prevent new operations being scheduled while it's waiting.
func (b *Balancer) FinishOps() {
	b.opsWG.Wait()
}

func (b *Balancer) PerformMove(r *ranje.Range) {

	// Find a node to place this range on.
	nid, err := b.rost.Candidate(r)

	// No candidates? That's a problem
	// TODO: Will result in quarantine? Might not be range's fault.
	// TODO: Should this maybe go back to Pending instead?
	if err != nil {
		log.Printf("error finding candidate: %v", err)
		// err := b.ks.RangeToState(r, ranje.PlaceError)
		// if err != nil {
		// 	panic(err)
		// }
		return
	}

	// Perform the placement in a background goroutine. (It's just a special
	// case of moving with no source.) When it terminates, the range will be
	// in the Ready or PlaceError states.
	err = operations.Run(&move.MoveOp{
		Keyspace: b.ks,
		Roster:   b.rost,
		Range:    r.Meta.Ident,
		Node:     nid,
	}, &b.opsWG)
	if err != nil {
		log.Printf("error scheduling MoveOp: %v", err)
	}
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}
