package balancer

import (
	"fmt"
	"log"
	"sort"
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

func (b *Balancer) RangesOnNodesWantingShutdown() []*ranje.Range {
	out := []*ranje.Range{}

	// TODO: Have the roster keep a list of nodes wanting shutdown rather than iterating.
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

	// Find any pending ranges and find any node to assign them to.
	for _, r := range b.ks.RangesByState(ranje.Pending) {
		b.PerformMove(r)
	}

	// Find any ranges in PlaceError and move them to Pending or Quarantine
	for _, r := range b.ks.RangesByState(ranje.PlaceError) {
		if r.NeedsQuarantine() {
			if err := b.ks.RangeToState(r, ranje.Quarantined); err != nil {
				panic(fmt.Sprintf("ToState: %s", err.Error()))
			}
			continue
		}

		if err := b.ks.RangeToState(r, ranje.Pending); err != nil {
			panic(fmt.Sprintf("ToState: %s", err.Error()))
		}
	}

	for _, r := range b.RangesOnNodesWantingShutdown() {
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
	nid := b.Candidate(r)

	// No candidates? That's a problem
	// TODO: Will result in quarantine? Might not be range's fault.
	// TODO: Should this maybe go back to Pending instead?
	if nid == "" {
		err := b.ks.RangeToState(r, ranje.PlaceError)
		if err != nil {
			panic(err)
		}
		return
	}

	// Perform the placement in a background goroutine. (It's just a special
	// case of moving with no source.) When it terminates, the range will be
	// in the Ready or PlaceError states.
	err := operations.Run(&move.MoveOp{
		Keyspace: b.ks,
		Roster:   b.rost,
		Range:    r.Meta.Ident,
		Node:     nid,
	}, &b.opsWG)
	if err != nil {
		log.Printf("Error placing pending range: %v", err)
	}
}

func (b *Balancer) Candidate(r *ranje.Range) string {
	b.rost.RLock()
	defer b.rost.RUnlock()

	// Build a list of nodes.
	// TODO: Just store them this way in roster.

	nodes := make([]*roster.Node, len(b.rost.Nodes))
	i := 0

	for _, nod := range b.rost.Nodes {
		nodes[i] = nod
		i += 1
	}

	// Filter nodes which are asking to be drained (probably shutting down).

	for i := range nodes {
		if nodes[i].WantDrain() {
			nodes[i] = nil
		}
	}

	// Remove excluded (i.e. nil) nodes

	{
		tmp := []*roster.Node{}

		for i := range nodes {
			if nodes[i] != nil {
				tmp = append(tmp, nodes[i])
			}
		}

		nodes = tmp
	}

	if len(nodes) == 0 {
		log.Printf("No non-excluded nodes available for range: %v", r)
		return ""
	}

	// Pick the node with lowest utilization.
	// TODO: This doesn't take into account ranges which are on the way to that
	//       node, and is generally totally insufficient.

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Utilization() < nodes[j].Utilization()
	})

	return nodes[0].Ident()
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}
