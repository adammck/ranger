package balancer

import (
	"log"
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
	dbg  *debugServer
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
	// TODO

	// Find any pending ranges and find any node to assign them to.
	// TODO

	// Find any ranges on nodes wanting drain, and move them.
	for _, r := range b.RangesOnNodesWantingDrain() {
		b.PerformMove(r)
	}
}

func (b *Balancer) PerformMove(r *ranje.Range) {
	panic("not implemented; see bbad4b6")
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}
