package balancer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/config"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc"
)

type Balancer struct {
	cfg  config.Config
	ks   *ranje.Keyspace
	rost *roster.Roster
	srv  *grpc.Server
	bs   *balancerServer
	dbg  *debugServer

	// Moves requested by operator (or by test)
	// To be applied next time Tick is called.
	opMoves   []OpMove
	opMovesMu sync.RWMutex

	// Same for splits.
	opSplits   map[ranje.Ident]OpSplit
	opSplitsMu sync.RWMutex

	// Same for joins.
	opJoins   []OpJoin
	opJoinsMu sync.RWMutex

	// TODO: Move this into the Tick method; just pass it along.
	rpcWG sync.WaitGroup
}

func New(cfg config.Config, ks *ranje.Keyspace, rost *roster.Roster, srv *grpc.Server) *Balancer {
	b := &Balancer{
		cfg:      cfg,
		ks:       ks,
		rost:     rost,
		srv:      srv,
		opMoves:  []OpMove{},
		opSplits: map[ranje.Ident]OpSplit{},
		opJoins:  []OpJoin{},
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

func (b *Balancer) Tick() {

	// Hold the keyspace lock for the entire tick.
	rs, unlock := b.ks.Ranges()
	defer unlock()

	// Any joins?

	func() {
		b.opJoinsMu.RLock()
		defer b.opJoinsMu.RUnlock()

		for _, j := range b.opJoins {
			r1, err := b.ks.Get(j.Left)
			if err != nil {
				log.Printf("join with invalid left side: %v (rID=%v)", err, j.Left)
				continue
			}

			r2, err := b.ks.Get(j.Right)
			if err != nil {
				log.Printf("join with invalid right side: %v (rID=%v)", err, j.Right)
				continue
			}

			r3, err := b.ks.JoinTwo(r1, r2)
			if err != nil {
				log.Printf("join failed: %v (left=%v, right=%v)", err, j.Left, j.Right)
				continue
			}

			log.Printf("new range from join: %v", r3)
		}

		b.opJoins = []OpJoin{}
	}()

	for _, r := range rs {
		b.tickRange(r)
	}

	// Now that we're finished advancing the ranges and placements, wait for any
	// RPCs emitted to complete.
	b.rpcWG.Wait()

	// TODO: Persist here, instead of after individual state updates?
}

func (b *Balancer) tickRange(r *ranje.Range) {
	switch r.State {
	case ranje.RsActive:

		// Not enough placements? Create one!
		if len(r.Placements) < b.cfg.Replication {

			nID, err := b.rost.Candidate(r, *ranje.AnyNode())
			if err != nil {
				log.Printf("error finding candidate node for %v: %v", r, err)
				return
			}

			p := ranje.NewPlacement(r, nID)
			r.Placements = append(r.Placements, p)

		}

		if opMove, ok := b.moveOp(r.Meta.Ident); ok {
			err := b.doMove(r, opMove)
			if err != nil {
				log.Printf("error initiating move: %v", err)

				// If the move was initiated by an operator, also forward the
				// error back to them.
				if opMove.Err != nil {
					opMove.Err <- err
					close(opMove.Err)
				}

				return
			}
		}

		// Range wants split?
		var opSplit *OpSplit
		func() {
			b.opSplitsMu.RLock()
			defer b.opSplitsMu.RUnlock()
			os, ok := b.opSplits[r.Meta.Ident]
			if ok {
				delete(b.opSplits, r.Meta.Ident)
				opSplit = &os
			}
		}()

		if opSplit != nil {
			b.ks.Split(r, opSplit.Key)
		}

	case ranje.RsSubsuming:
		err := b.ks.RangeCanBeObsoleted(r)
		if err != nil {
			log.Printf("may not be obsoleted: %v (p=%v)", err, r)
		} else {
			// No error, so ready to obsolete the range.
			b.ks.RangeToState(r, ranje.RsObsolete)
		}

	case ranje.RsObsolete:
		// TODO: Skip obsolete ranges in Tick. There's never anything to do with
		//       them, except possibly discard them, which we don't support yet.

	default:
		panic(fmt.Sprintf("unknown RangeState value: %s", r.State))
	}

	// Tick every placement.

	toDestroy := []int{}

	for i, p := range r.Placements {
		destroy := false
		b.tickPlacement(p, &destroy)
		if destroy {
			toDestroy = append(toDestroy, i)
		}
	}

	for _, idx := range toDestroy {
		r.Placements = append(r.Placements[:idx], r.Placements[idx+1:]...)
	}
}

func (b *Balancer) moveOp(rID ranje.Ident) (OpMove, bool) {
	b.opMovesMu.RLock()
	defer b.opMovesMu.RUnlock()

	// TODO: Incredibly dumb to iterate this list for every range. Do it once at
	//       the start of the Tick and stitch them back together or something!
	for i := range b.opMoves {
		if b.opMoves[i].Range == rID {
			tmp := b.opMoves[i]
			b.opMoves = append(b.opMoves[:i], b.opMoves[i+1:]...)
			return tmp, true
		}
	}

	return OpMove{}, false
}

func (b *Balancer) doMove(r *ranje.Range, opMove OpMove) error {
	var src *ranje.Placement
	if opMove.Src != "" {

		// Source node was given, so take placement from that.
		for _, p := range r.Placements {
			if p.NodeID == opMove.Src {
				src = p
				break
			}
		}

		if src == nil {
			return fmt.Errorf("src placement not found (rID=%v, nID=%v)", r.Meta.Ident, opMove.Src)
		}

	} else {

		// No source node given, so just take the first Ready placement.
		for _, p := range r.Placements {
			if p.State == ranje.PsReady {
				src = p
				break
			}
		}

		if src == nil {
			return fmt.Errorf("no ready placement found (rID=%v)", r.Meta.Ident)
		}
	}

	// If the source placement is already being replaced by some other
	// placement, reject the move.
	for _, p := range r.Placements {
		if p.IsReplacing == src.NodeID {
			return fmt.Errorf("placement already being replaced (src=%v, dest=%v)", src, p.NodeID)
		}
	}

	destNodeID, err := b.rost.Candidate(r, ranje.Constraint{NodeID: opMove.Dest})
	if err != nil {
		return err
	}

	// If the move was initiated by an operator (via RPC), then it will have an
	// error channel. When the move is complete, close to channel to unblock the
	// RPC handler.
	var cb func()
	if opMove.Err != nil {
		cb = func() {
			close(opMove.Err)
		}
	}

	p := ranje.NewReplacement(r, destNodeID, src.NodeID, cb)
	r.Placements = append(r.Placements, p)

	return nil
}

// using a weird bool pointer arg to avoid having to return false from every
// place except one.
func (b *Balancer) tickPlacement(p *ranje.Placement, destroy *bool) {

	// Get the node that this placement is on.
	// (This is a problem, in most states.)
	n := b.rost.NodeByIdent(p.NodeID)
	if p.State != ranje.PsGiveUp && p.State != ranje.PsDropped {
		if n == nil {
			// The node has disappeared.
			log.Printf("missing node: %s", p.NodeID)
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}
	}

	// If this placement is replacing another, and that placement is gone from
	// the keyspace, then clear the annotation. (Note that we don't care what
	// the roster says; this is just cleanup.)
	if p.IsReplacing != "" {
		found := false
		for _, pp := range p.Range().Placements {
			if p.IsReplacing == pp.NodeID {
				found = true
				break
			}
		}

		if !found {
			p.DoneReplacing()
		}
	}

	// If the node this placement is on wants to be drained, mark this placement
	// as wanting to be moved. The next Tick will create a new placement, and
	// exclude the current node from the candidates.
	//
	// TODO: This whole WantMove thing was a hack to initiate moves in tests,
	//       get rid of it and probably replace it with a on the balancer?
	//
	// TODO: Also this is almost certainly only valid in some placement states;
	//       think about that.
	if n.WantDrain() {
		func() {
			b.opMovesMu.Lock()
			defer b.opMovesMu.Unlock()

			// TODO: Probably add a method to do this.
			b.opMoves = append(b.opMoves, OpMove{
				Range: p.Range().Meta.Ident,
				Src:   n.Ident(),
			})
		}()

		// TODO: Fix
		//p.SetWantMoveTo(ranje.AnyNode())
	}

	switch p.State {
	case ranje.PsPending:
		// If the node already has the range (i.e. this is not the first tick
		// where the placement is PsPending, so the RPC may already have been
		// sent), check its remote state, which may have been updated by a
		// response to a Give or by a periodic probe. We may be able to advance.
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case state.NsPreparing:
				log.Printf("node %s still preparing %s", n.Ident(), p.Range().Meta.Ident)

			case state.NsPrepared:
				b.ks.PlacementToState(p, ranje.PsPrepared)
				return

			case state.NsPreparingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.
				log.Printf("error placing %s on %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			}
		} else {
			log.Printf("will give %s to %s", p.Range().Meta.Ident, n.Ident())
		}

		// Send a Give RPC (maybe not the first time; once per tick).
		// TODO: Keep track of how many times we've tried this and for how long.
		//       We'll want to give up if it takes too long to prepare.
		b.RPC(func() {
			err := n.Give(context.Background(), p, getParents(b.ks, b.rost, p.Range()))
			if err != nil {
				log.Printf("error giving %v to %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsPrepared:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {
			// The node doesn't have the placement any more! Abort.
			log.Printf("placement missing from node (rID=%s, n=%s)", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		switch ri.State {
		case state.NsPrepared:
			// This is the first time around.
			log.Printf("will instruct %s to serve %s", n.Ident(), p.Range().Meta.Ident)

		case state.NsReadying:
			// We've already sent the Serve RPC at least once, and the node
			// is working on it. Just keep waiting.
			log.Printf("node %s still readying %s", n.Ident(), p.Range().Meta.Ident)

		case state.NsReadyingError:
			// TODO: Pass back more information from the node, here. It's
			//       not an RPC error, but there was some failure which we
			//       can log or handle here.l
			log.Printf("error readying %s on %s", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)

		case state.NsReady:
			b.ks.PlacementToState(p, ranje.PsReady)
			return

		// TODO: state.NsReadyError?

		default:
			log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		// We are ready to move from Prepared to Ready, but may have to wait for
		// the placement that this is replacing (maybe) to relinquish it first.
		if !b.ks.PlacementMayBecomeReady(p) {
			return
		}

		b.RPC(func() {
			err := n.Serve(context.Background(), p)
			if err != nil {
				log.Printf("error serving %v to %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsReady:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {
			// The node doesn't have the placement any more! Abort.
			log.Printf("placement missing from node (rID=%s, n=%s)", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		switch ri.State {
		case state.NsReady:
			log.Printf("ready: %s", p.LogString())

		case state.NsTaking:
			log.Printf("node %s still taking %s", n.Ident(), p.Range().Meta.Ident)

		case state.NsTakingError:
			// TODO: Pass back more information from the node, here. It's
			//       not an RPC error, but there was some failure which we
			//       can log or handle here.
			log.Printf("error taking %s from %s", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return

		case state.NsTaken:
			b.ks.PlacementToState(p, ranje.PsTaken)
			return

		// TODO: state.NsTakeError?

		default:
			log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
			b.ks.PlacementToState(p, ranje.PsGiveUp)
			return
		}

		if !b.ks.PlacementMayBeTaken(p) {
			return
		}

		b.RPC(func() {
			err := n.Take(context.Background(), p)
			if err != nil {
				log.Printf("error taking %v from %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsTaken:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {

			// We can assume that the range has been dropped when the roster no
			// longer has any info about it. This can happen in response to a
			// Drop RPC or a normal probe cycle. Either way, the range is gone
			// from the node. Nodes will never confirm that they used to have a
			// range but and have dropped it.
			b.ks.PlacementToState(p, ranje.PsDropped)
			return

		} else {
			switch ri.State {
			case state.NsTaken:
				if err := b.ks.PlacementMayBeDropped(p); err != nil {
					return
				}

			case state.NsDropping:
				// We have already decided to drop the range, and have probably sent
				// the RPC (below) already, so cannot turn back now.
				log.Printf("node %s still dropping %s", n.Ident(), p.Range().Meta.Ident)

			case state.NsDroppingError:
				// TODO: Pass back more information from the node, here. It's
				//       not an RPC error, but there was some failure which we
				//       can log or handle here.
				log.Printf("error dropping %s from %s", p.Range().Meta.Ident, n.Ident())
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.State)
				b.ks.PlacementToState(p, ranje.PsGiveUp)
				return
			}
		}

		b.RPC(func() {
			err := n.Drop(context.Background(), p)
			if err != nil {
				log.Printf("error dropping %v from %s: %v", p.LogString(), n.Ident(), err)
			}
		})

	case ranje.PsGiveUp:
		// This transition only exists to provide an error-handling path to
		// PsDropped without sending any RPCs.
		log.Printf("giving up on %s", p.LogString())
		b.ks.PlacementToState(p, ranje.PsDropped)
		return

	case ranje.PsDropped:
		log.Printf("will destroy %s", p.LogString())
		*destroy = true
		return

	default:
		panic(fmt.Sprintf("unhandled PlacementState value: %s", p.State))
	}
}

func (b *Balancer) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}

func (b *Balancer) RPC(f func()) {
	b.rpcWG.Add(1)

	go func() {
		f()
		b.rpcWG.Done()
	}()
}

func getParents(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range) []*pb.Parent {
	parents := []*pb.Parent{}
	seen := map[ranje.Ident]struct{}{}
	addParents(ks, rost, rang, &parents, seen)
	return parents
}

func addParents(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, parents *[]*pb.Parent, seen map[ranje.Ident]struct{}) {

	// Don't bother serializing the same placement many times. (The range tree
	// won't have cycles, but is also not a DAG.)
	_, ok := seen[rang.Meta.Ident]
	if ok {
		return
	}

	*parents = append(*parents, pbPlacement(rost, rang))
	seen[rang.Meta.Ident] = struct{}{}

	for _, rID := range rang.Parents {
		r, err := ks.Get(rID)
		if err != nil {
			// TODO: Think about how to recover from this. It's bad.
			panic(fmt.Sprintf("getting range with ident %v: %v", rID, err))
		}

		addParents(ks, rost, r, parents, seen)
	}
}

func pbPlacement(rost *roster.Roster, r *ranje.Range) *pb.Parent {

	// TODO: The kv example doesn't care about range history, because it has no
	//       external write log, so can only fetch from nodes. So we can skip
	//       sending them at all. Maybe add a controller feature flag?
	//

	pbPlacements := make([]*pb.Placement, len(r.Placements))

	for i, p := range r.Placements {
		n := rost.NodeByIdent(p.NodeID)

		node := ""
		if n != nil {
			node = n.Addr()
		}

		pbPlacements[i] = &pb.Placement{
			Node:  node,
			State: p.State.ToProto(),
		}
	}

	return &pb.Parent{
		Range:      r.Meta.ToProto(),
		Placements: pbPlacements,
	}
}
