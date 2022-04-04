package balancer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/config"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
)

type Balancer struct {
	cfg  config.Config
	ks   *ranje.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer
	dbg  *debugServer

	// Funcs to be called at the end of the current tick.
	cb    []func()
	cbMu  sync.RWMutex
	rpcWG sync.WaitGroup

	// Just for testing.
	lastTickRPCs uint32
}

func New(cfg config.Config, ks *ranje.Keyspace, rost *roster.Roster, srv *grpc.Server) *Balancer {
	b := &Balancer{
		cfg:  cfg,
		ks:   ks,
		rost: rost,
		srv:  srv,
		cb:   []func(){},
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

// TODO: Would be better to return a slice of {give, serve, take, drop} or
//       something, to ensure that we're seeing the right kind of RPCs.
func (b *Balancer) LastTickRPCs() int {
	return int(atomic.LoadUint32(&b.lastTickRPCs))
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
	log.Print("tick")
	atomic.StoreUint32(&b.lastTickRPCs, 0)

	rs, unlock := b.ks.Ranges()
	defer unlock()

	for _, r := range rs {
		b.tickRange(r)
	}

	// Now that we're finished advancing the ranges and placements, wait for any
	// RPCs emitted to complete.
	b.rpcWG.Wait()

	b.callbacks()

	// Find any unknown and complain about them. There should be NONE of these
	// in the keyspace; it indicates a state bug.
	// for _, r := range b.ks.RangesByState(ranje.RsUnknown) {
	// 	log.Fatalf("range in unknown state: %v", r)
	// }

	// Find any placements on nodes which are unknown to the roster. This can
	// happen if a node goes away while the controller is down, and probably
	// other state snafus.
	// TODO

	// Find any pending ranges and find any node to assign them to.
	// TODO

	// Find any ranges on nodes wanting drain, and move them.
	// for _, r := range b.RangesOnNodesWantingDrain() {
	// 	b.PerformMove(r)
	// }
}

func (b *Balancer) callbacks() {
	b.cbMu.Lock()
	defer b.cbMu.Unlock()

	for _, f := range b.cb {
		f()
	}

	b.cb = []func(){}
}

func (b *Balancer) Queue(f func()) {
	b.cbMu.Lock()
	defer b.cbMu.Unlock()
	b.cb = append(b.cb, f)
}

func (b *Balancer) tickRange(r *ranje.Range) {
	switch r.State {
	case ranje.RsActive:

		// Not enough placements? Create one!
		if len(r.Placements) < b.cfg.Replication {

			nID, err := b.rost.Candidate(r)
			if err != nil {
				log.Printf("error finding candidate node for %v: %v", r, err)
				return
			}

			// TODO: Find a candidate node before creating the placement.
			p := ranje.NewPlacement(r, nID)
			r.Placements = append(r.Placements, p)

		}

	default:
		panic(fmt.Sprintf("unknown RangeState value: %s", r.State))
	}

	for _, p := range r.Placements {
		b.tickPlacement(p)
	}
}

func (b *Balancer) tickPlacement(p *ranje.Placement) {
	switch p.State {
	case ranje.PsPending:
		n := b.rost.NodeByIdent(p.NodeID)
		if n == nil {
			// The node has disappeared since it was selected.
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		// If the node already has the range (i.e. this is not the first tick
		// where the placement is PsPending, so the RPC may already have been
		// sent)

		// If we know that the node already has the placement (i.e. this is not
		// the first tick), check the remote state. I
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case roster.NsPreparing:
				log.Printf("node %s still preparing %s", n.Ident(), p.Range().Meta.Ident)
			case roster.NsPrepared:
				b.ks.PlacementToState(p, ranje.PsPrepared)
				return
			case roster.NsPreparingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.
				log.Printf("error placing %s on %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			default:
				log.Printf("unexpected state: %s", ri.State)
			}
		}

		// Send a Give RPC (maybe not the first time; once per tick).
		b.RPC(func() {
			err := n.Give(context.Background(), p)
			if err != nil {
				log.Printf("error giving %v to %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsLoading:
		n := b.rost.NodeByIdent(p.NodeID)
		if n == nil {
			// The node disappeared while we were waiting for it to load the
			// range.
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		rID := p.Range().Meta.Ident
		ri, ok := n.Get(rID)
		if !ok {
			// The node seems to have forgotten about the range while we were
			// waiting for it to load. (This should not happen, but perhaps
			// indicates that the node decided that it didn't have enough
			// capacity after all.)
			log.Printf("range went away in PsLoading: rID=%s, nID=%s", rID, p.NodeID)
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		if ri.State == roster.NsPrepared {
			b.ks.PlacementToState(p, ranje.PsReady)
			return
		}

	default:
		panic(fmt.Sprintf("unknown PlacementState value: %s", p.State))
	}
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}

func (b *Balancer) RPC(f func()) {
	atomic.AddUint32(&b.lastTickRPCs, 1)
	b.rpcWG.Add(1)

	go func() {
		f()
		b.rpcWG.Done()
	}()
}
