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

func (b *Balancer) Tick() {
	// Find any unknown and complain about them. There should be NONE of these
	// in the keyspace; it indicates a state bug.
	for _, r := range b.ks.RangesByState(ranje.Unknown) {
		log.Fatalf("range in unknown state: %v", r)
	}

	// Find any pending ranges and find any node to assign them to.
	for _, r := range b.ks.RangesByState(ranje.Pending) {
		//r.MustState(ranje.Placing)

		// Find a node to place this range on.
		nid := b.Candidate(r)

		// No candidates? That's a problem
		// TODO: Will result in quarantine? Might not be range's fault.
		if nid == "" {
			err := b.ks.ToState(r, ranje.PlaceError)
			if err != nil {
				panic(err)
			}
			continue
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

	// Find any ranges in PlaceError and move them to Pending or Quarantine
	for _, r := range b.ks.RangesByState(ranje.PlaceError) {
		if r.NeedsQuarantine() {
			if err := b.ks.ToState(r, ranje.Quarantined); err != nil {
				panic(fmt.Sprintf("ToState: %s", err.Error()))
			}
			continue
		}

		if err := b.ks.ToState(r, ranje.Pending); err != nil {
			panic(fmt.Sprintf("ToState: %s", err.Error()))
		}
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

func (b *Balancer) Candidate(r *ranje.Range) string {
	b.rost.RLock()
	defer b.rost.RUnlock()

	var best string

	nIDs := make([]string, len(b.rost.Nodes))
	i := 0

	for nID := range b.rost.Nodes {
		nIDs[i] = nID
		i += 1
	}

	sort.Strings(nIDs)

	// lol
	for _, nID := range nIDs {
		best = nID
		break
	}

	// No suitable nodes?
	if best == "" {
		log.Printf("No candidates for range: %v", r)
		return ""
	}

	return best
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}
