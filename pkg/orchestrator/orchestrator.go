package orchestrator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc"
)

type Orchestrator struct {
	cfg  config.Config
	ks   *keyspace.Keyspace
	rost *roster.Roster // TODO: Use simpler interface, not whole Roster.
	srv  *grpc.Server

	bs  *orchestratorServer
	dbg *debugServer

	// Moves requested by operator (or by test)
	// To be applied next time Tick is called.
	opMoves   []OpMove
	opMovesMu sync.RWMutex

	// Same for splits.
	// TODO: Why is this a map??
	opSplits   map[api.Ident]OpSplit
	opSplitsMu sync.RWMutex

	// Same for joins.
	opJoins   []OpJoin
	opJoinsMu sync.RWMutex
}

func New(cfg config.Config, ks *keyspace.Keyspace, rost *roster.Roster, srv *grpc.Server) *Orchestrator {
	b := &Orchestrator{
		cfg:      cfg,
		ks:       ks,
		rost:     rost,
		srv:      srv,
		opMoves:  []OpMove{},
		opSplits: map[api.Ident]OpSplit{},
		opJoins:  []OpJoin{},
	}

	// Register the gRPC server to receive instructions from operators. This
	// will hopefully not be necessary once balancing actually works!
	b.bs = &orchestratorServer{orch: b}
	pb.RegisterOrchestratorServer(srv, b.bs)

	// Register the debug server, to fetch info about the state of the world.
	// One could arguably pluck this straight from Consul -- since it's totally
	// consistent *right?* -- but it's a much richer interface to do it here.
	b.dbg = &debugServer{orch: b}
	pb.RegisterDebugServer(srv, b.dbg)

	return b
}

func (b *Orchestrator) Tick() {

	// Hold the keyspace lock for the entire tick.
	rs, unlock := b.ks.Ranges()
	defer unlock()

	// Any joins?
	// TODO: Extract this to a function.

	func() {
		b.opJoinsMu.RLock()
		defer b.opJoinsMu.RUnlock()

		for _, opJoin := range b.opJoins {

			fail := func(err error) {
				log.Print(err.Error())
				if opJoin.Err != nil {
					opJoin.Err <- err
					close(opJoin.Err)
				}
			}

			r1, err := b.ks.Get(opJoin.Left)
			if err != nil {
				fail(fmt.Errorf("join with invalid left side: %v (rID=%v)", err, opJoin.Left))
				continue
			}

			r2, err := b.ks.Get(opJoin.Right)
			if err != nil {
				fail(fmt.Errorf("join with invalid right side: %v (rID=%v)", err, opJoin.Right))
				continue
			}

			// Find the candidate for the new (joined) range before performing
			// the join. Once that happens, we can't (currently) abort.
			c := ranje.AnyNode
			if opJoin.Dest != "" {
				c = ranje.Constraint{NodeID: opJoin.Dest}
			}
			nIDr3, err := b.rost.Candidate(nil, c)
			if err != nil {
				fail(fmt.Errorf("error selecting join candidate: %v", err))
				continue
			}

			r3, err := b.ks.JoinTwo(r1, r2)
			if err != nil {
				fail(fmt.Errorf("join failed: %v (left=%v, right=%v)", err, opJoin.Left, opJoin.Right))
				continue
			}

			log.Printf("new range from join: %v", r3)

			// If we made it this far, the join has happened and already been
			// persisted. No turning back now.

			p := ranje.NewPlacement(r3, nIDr3)
			r3.Placements = append(r3.Placements, p)

			// Unlock operator RPC if applicable.
			// Note that this will only fire if *this* placement becomes ready.
			// If it fails, and is replaced, and that succeeds, the RPC will
			// never unblock.
			//
			// TODO: Move this to range.OnReady, which should only fire when
			//       the minReady number of placements are ready.
			//
			if opJoin.Err != nil {
				p.OnReady(func() {
					close(opJoin.Err)
				})
			}
		}

		b.opJoins = []OpJoin{}
	}()

	// Keep track of which ranges we've already ticked, since we do those
	// involved in ops first.
	visited := map[api.Ident]struct{}{}

	ops, err := b.ks.Operations()
	if err == nil {
		for _, op := range ops {

			// Complete the operation if we can. This marks all of the parent
			// ranges as RsObsolete if their placements have all been dropped.
			_, err = op.CheckComplete(b.ks)
			if err != nil {
				log.Printf("error completing operation: %v", err)
			}

			// Note that we don't return here; we still tick the now-obsolete
			// ranges rather than introduce weird rules about which ranges will
			// and will not be ticked.

			for _, r := range op.Ranges() {
				b.tickRange(r, op)
				visited[r.Meta.Ident] = struct{}{}
			}
		}
	} else {
		// TODO: Once ticks are cleanly abortable, return err instead of this.
		log.Printf("error reading in-flight operations: %v", err)
	}

	// Iterate over all ranges... or at least all the ranges which existed when
	// keyspace.Ranges returned, which doesn't include any that we just created
	// from joins above, or any we create from splits while ticking. This is a
	// big mess.
	for _, r := range rs {

		// Don't bother ticking Obsolete ranges. They never change.
		// TODO: Don't include them in the first place!
		if r.State == api.RsObsolete {
			continue
		}

		// Skip the range if it has already been ticked by the operations loop,
		// above. I think we need to refactor this.
		if _, ok := visited[r.Meta.Ident]; ok {
			continue
		}

		b.tickRange(r, nil)
	}

	// TODO: Persist here, instead of after individual state updates?
}

func (b *Orchestrator) tickRange(r *ranje.Range, op *keyspace.Operation) {
	switch r.State {
	case api.RsActive:

		// Not enough placements? Create one!
		if len(r.Placements) < b.cfg.Replication {

			nID, err := b.rost.Candidate(r, ranje.AnyNode)
			if err != nil {
				log.Printf("error finding candidate node for %v: %v", r, err)
				return
			}

			p := ranje.NewPlacement(r, nID)
			r.Placements = append(r.Placements, p)
		}

		// Pending move for this range?
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

			// Find candidates for the left and right sides *before* performing
			// the split. Once that happens, we can't (currently) abort.
			//
			// TODO: Allow split abort by allowing ranges to transition back
			//       from RsSubsuming into RsActive, and from RsActive into some
			//       new terminal state (RsAborted?) like RsObsolete. Would also
			//       need a new entry state (RsNewSplit?) to indicate that it's
			//       okay to give up, unlike RsActive. Ranges would need to keep
			//       track of how many placements had been created and failed.

			c := ranje.AnyNode
			if opSplit.Left != "" {
				c = ranje.Constraint{NodeID: opSplit.Left}
			}
			nIDL, err := b.rost.Candidate(nil, c)
			if err != nil {
				log.Printf("error selecting split candidate left: %v", err)
				if opSplit.Err != nil {
					opSplit.Err <- err
					close(opSplit.Err)
				}
				return
			}

			c = ranje.AnyNode
			if opSplit.Right != "" {
				c = ranje.Constraint{NodeID: opSplit.Right}
			}
			nIDR, err := b.rost.Candidate(nil, c)
			if err != nil {
				log.Printf("error selecting split candidate right: %v", err)
				if opSplit.Err != nil {
					opSplit.Err <- err
					close(opSplit.Err)
				}
				return
			}

			// Perform the actual range split. The source range (r) is moved to
			// RsSubsuming, where it will remain until its placements have all
			// been moved elsewhere. Two new ranges
			rL, rR, err := b.ks.Split(r, opSplit.Key)

			if err != nil {
				log.Printf("error initiating split: %v", err)
				if opSplit.Err != nil {
					opSplit.Err <- err
					close(opSplit.Err)
				}
				return
			}

			// If we made it this far, the split has happened and already been
			// persisted.

			// TODO: We're creating placements here on a range which is NOT the
			//       one we're ticking. That seems... okay? We hold the keyspace
			//       lock for the whole tick. But think about edge cases?
			//       -
			//       We could leave some turd like NextPlacementNodeID on the
			//       range and let the first clause (no placements?) in this
			//       method pick it up, but (a) that's gross, and (b) what if
			//       some later range gets placed on the node before that
			//       happens? All worse options.
			//       -
			//       Actually I think we need to extract this chunk of code up
			//       into a "ranges which have splits scheduled" loop before the
			//       main all-ranges loop. Join is already up there.

			pL := ranje.NewPlacement(rL, nIDL)
			rL.Placements = append(rL.Placements, pL)

			pR := ranje.NewPlacement(rR, nIDR)
			rR.Placements = append(rR.Placements, pR)

			// If the split was initiated by an operator (via RPC), then it will
			// have an error channel. When the split is complete (i.e. the range
			// becomes obsolete) close to channel to unblock the RPC handler.
			if opSplit.Err != nil {
				r.OnObsolete(func() {
					close(opSplit.Err)
				})
			}
		}

	case api.RsSubsuming:
		// Skip parent ranges of operations in flight. The only thing to do is
		// check whether they're complete, which we do before calling tick.

	case api.RsObsolete:
		// TODO: Skip obsolete ranges in Tick. There's never anything to do with
		//       them, except possibly discard them, which we don't support yet.

	default:
		panic(fmt.Sprintf("unknown RangeState value: %s", r.State))
	}

	// Tick every placement.

	toDestroy := []int{}

	for i, p := range r.Placements {
		if b.tickPlacement(p, r, op) {
			toDestroy = append(toDestroy, i)
		}
	}

	for _, idx := range toDestroy {
		r.Placements = append(r.Placements[:idx], r.Placements[idx+1:]...)
	}
}

func (b *Orchestrator) moveOp(rID api.Ident) (OpMove, bool) {
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

func (b *Orchestrator) doMove(r *ranje.Range, opMove OpMove) error {
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
			if p.StateCurrent == api.PsActive {
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

func (b *Orchestrator) tickPlacement(p *ranje.Placement, r *ranje.Range, op *keyspace.Operation) (destroy bool) {
	destroy = false

	// Get the node that this placement is on.
	// (This is a problem, in most states.)
	n := b.rost.NodeByIdent(p.NodeID)
	if p.StateCurrent != api.PsGiveUp && p.StateCurrent != api.PsDropped {
		if n == nil {
			// The node has disappeared.
			log.Printf("missing node: %s", p.NodeID)
			b.ks.PlacementToState(p, api.PsGiveUp)
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
	// TODO: Also this is almost certainly only valid in some placement states;
	//       think about that.
	if n != nil && n.WantDrain() {
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

	switch p.StateCurrent {
	case api.PsPending:
		doPlace := false

		// If the node already has the range (i.e. this is not the first tick
		// where the placement is PsPending, so the RPC may already have been
		// sent), check its remote state, which may have been updated by a
		// response to a Give or by a periodic probe. We may be able to advance.
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case api.NsLoading:
				log.Printf("node %s still loading %s", n.Ident(), p.Range().Meta.Ident)
				p.Want(api.PsInactive)

			case api.NsInactive:
				b.ks.PlacementToState(p, api.PsInactive)

			case api.NsNotFound:
				// Special case: Give has already been attempted, but it failed.
				// We can try again, same as if the placement was missing.
				doPlace = true

			default:
				log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.StateCurrent)
				b.ks.PlacementToState(p, api.PsGiveUp)
			}
		} else {
			doPlace = true
		}

		if doPlace {
			p.Want(api.PsInactive)
			if p.FailedGive {
				destroy = true
			}
		}

	case api.PsInactive:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {

			// The node doesn't have the placement any more! Maybe we tried to
			// activate it but gave up.
			if p.FailedActivate {
				destroy = true
				return
			}

			// Maybe we dropped it on purpose because it's been subsumed.
			if op.MayDrop(p, r) == nil {
				b.ks.PlacementToState(p, api.PsDropped)
				return
			}

			// Otherwise, abort. It's been forgotten.
			log.Printf("placement missing from node (rID=%s, n=%s)", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, api.PsGiveUp)
			return
		}

		switch ri.State {
		case api.NsInactive:

			// This is the first time around. In order for this placement to
			// move to Ready, the one it is replacing (maybe) must reliniquish
			// it first.
			if err := op.MayActivate(p, r); err == nil {
				p.Want(api.PsActive)

				return
			} else {
				log.Printf("will not serve (rID=%s, n=%s, err=%s)", p.Range().Meta.Ident, n.Ident(), err)
			}

			// We are ready to move from Inactive to Dropped, but we have to wait
			// for the placement(s) that are replacing this to become Ready.
			if err := op.MayDrop(p, r); err == nil {
				p.Want(api.PsDropped)
			} else {
				log.Printf("will not drop (rID=%s, n=%s, err=%s)", p.Range().Meta.Ident, n.Ident(), err)
			}

			return

		case api.NsActivating:
			// We've already sent the Serve RPC at least once, and the node is
			// working on it. Just keep waiting. But send another Serve RPC to
			// check whether it's finished and is now Ready. (Or has changed to
			// some other state through crash or bug.)
			log.Printf("node %s still readying %s", n.Ident(), p.Range().Meta.Ident)
			p.Want(api.PsActive)

		case api.NsDropping:
			// This placement failed to serve too many times. We've given up on it.
			log.Printf("node %s still dropping %s", n.Ident(), p.Range().Meta.Ident)
			p.Want(api.PsDropped)

		case api.NsActive:
			b.ks.PlacementToState(p, api.PsActive)

		default:
			log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.StateCurrent)
			b.ks.PlacementToState(p, api.PsGiveUp)
			return
		}

	case api.PsActive:
		doTake := false

		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {
			// The node doesn't have the placement any more! Abort.
			log.Printf("placement missing from node (rID=%s, n=%s)", p.Range().Meta.Ident, n.Ident())
			b.ks.PlacementToState(p, api.PsGiveUp)
			return
		}

		switch ri.State {
		case api.NsActive:
			doTake = true

		case api.NsDeactivating:
			log.Printf("node %s still deactivating %s", n.Ident(), p.Range().Meta.Ident)
			p.Want(api.PsInactive)

		case api.NsInactive:
			b.ks.PlacementToState(p, api.PsInactive)

		default:
			log.Printf("very unexpected remote state: %s (placement state=%s)", ri.State, p.StateCurrent)
			b.ks.PlacementToState(p, api.PsGiveUp)
		}

		if doTake {
			if err := op.MayDeactivate(p, r); err == nil {
				p.Want(api.PsInactive)

			} else {
				log.Printf("will not deactivate (rID=%s, n=%s, err=%s)", p.Range().Meta.Ident, n.Ident(), err)
			}
		}

	case api.PsGiveUp:
		// This transition only exists to provide an error-handling path to
		// PsDropped without sending any RPCs.
		log.Printf("giving up on %s", p.LogString())
		b.ks.PlacementToState(p, api.PsDropped)
		return

	case api.PsDropped:
		log.Printf("will destroy %s", p.LogString())
		destroy = true
		return

	default:
		panic(fmt.Sprintf("unhandled PlacementState value: %s", p.StateCurrent))
	}

	return
}

func (b *Orchestrator) Run(t *time.Ticker) {
	for ; true; <-t.C {
		b.Tick()
	}
}
