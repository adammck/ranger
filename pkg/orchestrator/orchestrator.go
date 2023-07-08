package orchestrator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"
)

type Orchestrator struct {
	c clockwork.Clock

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
	opSplits   map[api.RangeID]OpSplit
	opSplitsMu sync.RWMutex

	// Same for joins.
	opJoins   []OpJoin
	opJoinsMu sync.RWMutex
}

func New(clock clockwork.Clock, ks *keyspace.Keyspace, rost *roster.Roster, srv *grpc.Server) *Orchestrator {
	b := &Orchestrator{
		c:        clock,
		ks:       ks,
		rost:     rost,
		srv:      srv,
		opMoves:  []OpMove{},
		opSplits: map[api.RangeID]OpSplit{},
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
	func() {
		b.opJoinsMu.RLock()
		defer b.opJoinsMu.RUnlock()

		for _, opJoin := range b.opJoins {
			b.initJoin(opJoin)
		}

		b.opJoins = []OpJoin{}
	}()

	// Keep track of which ranges we've already ticked, since we do those
	// involved in ops first.
	visited := map[api.RangeID]struct{}{}

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (b *Orchestrator) tickRange(r *ranje.Range, op *keyspace.Operation) {
	switch r.State {
	case api.RsActive:

		// Not enough placements? Create enough to reach the minimum.
		if n := min(r.MinPlacements(), r.TargetActive()) - len(r.Placements); n > 0 {
			con := ranje.Constraint{}

			for i := 0; i < n; i++ {
				nID, err := b.rost.Candidate(r, con)
				if err != nil {
					//log.Printf("no candidate for: rID=%s, con=%v, err=%v", r, con, err)
					continue
				}

				con = con.WithNot(nID)
				r.NewPlacement(nID)
			}
		}

		// Initiate any pending moves for this range.
		for {

			// Can't move if we're already at the max placements. The fact that
			// moveOp results in a new placement is... just known.
			//
			// TODO: This is a hack and doesn't belong here. Move ops should be
			// broken into adds and taints.
			if len(r.Placements) >= r.MaxPlacements() {
				break
			}

			opMove, ok := b.moveOp(r.Meta.Ident)
			if !ok {
				break
			}

			err := b.doMove(r, opMove)
			if err != nil {
				// If the move was initiated by an operator, also forward the
				// error back to them.
				if opMove.Err != nil {
					opMove.Err <- err
					close(opMove.Err)
				}
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
			b.initSplit(r, *opSplit)
		}

	case api.RsSubsuming:
		// Skip parent ranges of operations in flight. The only thing to do is
		// check whether they're complete, which we do before calling tick.

	case api.RsObsolete:
		// TODO: Skip obsolete ranges in Tick. There's never anything to do with
		// them, except possibly discard them, which we don't support yet.

	default:
		panic(fmt.Sprintf("unknown RangeState value: %s", r.State))
	}

	// Tick every placement.

	toDestroy := []*ranje.Placement{}

	for _, p := range r.Placements {
		if b.tickPlacement(p, r, op) {
			toDestroy = append(toDestroy, p)
		}
	}

	for _, p := range toDestroy {
		r.DestroyPlacement(p)
	}
}

func (b *Orchestrator) moveOp(rID api.RangeID) (OpMove, bool) {
	b.opMovesMu.RLock()
	defer b.opMovesMu.RUnlock()

	// TODO: Incredibly dumb to iterate this list for every range. Do it once at
	// the start of the Tick and stitch them back together or something!
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
			return fmt.Errorf("no active placement found (rID=%v)", r.Meta.Ident)
		}
	}

	// If the src is already tainted, it might be being replaced by some other
	// placement already. (The operator must manually remove the taint if that
	// really isn't the case.)
	//
	// TODO: Add an endpoint to remove the taint.
	if src.Tainted {
		return fmt.Errorf("src placement is already tainted (rID=%s, src=%s)", r.Meta.Ident, src.NodeID)
	}

	destNodeID, err := b.rost.Candidate(r, ranje.Constraint{NodeID: opMove.Dest})
	if err != nil {
		return err
	}

	// If the move was initiated by an operator (via RPC), then it will have an
	// error channel. When the source range is ready to be destroyed (i.e. the
	// move is complete), close to channel to unblock the RPC handler.
	if opMove.Err != nil {
		src.OnDestroy(func() {
			close(opMove.Err)
		})
	}

	r.NewPlacement(destNodeID)

	// Taint the source range, to provide a hint to the orchestrator that it
	// should deactivate and drop itself asap (i.e. when the replacement,
	// created just above, becomes ready to activate in its place.)
	src.Tainted = true

	return nil
}

func (b *Orchestrator) tickPlacement(p *ranje.Placement, r *ranje.Range, op *keyspace.Operation) (destroy bool) {
	destroy = false

	// Get the node that this placement is on. If the node couldn't be fetched,
	// it's probably crashed, so move the placement to Missing so it's replaced.
	n, err := b.rost.NodeByIdent(p.NodeID)
	if err != nil {
		if p.StateCurrent != api.PsMissing && p.StateCurrent != api.PsDropped {
			b.ks.PlacementToState(p, api.PsMissing)
			return
		}
	}

	// If the node this placement is on wants to be drained, mark this placement
	// as wanting to be moved. The next Tick will create a new placement, and
	// exclude the current node from the candidates.
	//
	// TODO: Also this is almost certainly only valid in some placement states;
	// think about that.
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
		// response to a Prepare or by a periodic probe. We may be able to
		// advance.
		ri, ok := n.Get(p.Range().Meta.Ident)
		if ok {
			switch ri.State {
			case api.NsPreparing:
				p.Want(api.PsInactive)

			case api.NsInactive:
				b.ks.PlacementToState(p, api.PsInactive)

			case api.NsNotFound:
				// Special case: Prepare has already been attempted, but it
				// failed. We can try again, as if the placement was missing.
				doPlace = true

			default:
				log.Printf("unexpected remote state: ris=%s, psc=%s", ri.State, p.StateCurrent)
				b.ks.PlacementToState(p, api.PsMissing)
			}
		} else {
			doPlace = true
		}

		if doPlace {
			p.Want(api.PsInactive)
			if p.Failed(api.Prepare) {
				destroy = true
			}
		}

	case api.PsInactive:
		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {

			// The node doesn't have the placement any more! Maybe we tried to
			// activate it but gave up.
			if p.Failed(api.Activate) {
				b.ks.PlacementLease(p, time.Time{}) // TODO: Better method
				destroy = true
				return
			}

			// Maybe we dropped it on purpose because it's been subsumed.
			if op.MayDrop(p, r) == nil {
				b.ks.PlacementToState(p, api.PsDropped)
				return
			}

			// Otherwise, abort. It's been forgotten.
			b.ks.PlacementToState(p, api.PsMissing)
			return
		}

		switch ri.State {
		case api.NsInactive:

			// Failed, so take back the lease.
			if p.Failed(api.Activate) {
				b.ks.PlacementLease(p, time.Time{}) // TODO: Better method
				return
			}

			// This is the first time around. In order for this placement to
			// move to Ready, the one it is replacing (maybe) must reliniquish
			// it first.
			if err := op.MayActivate(p, r); err == nil {
				b.ks.PlacementLease(p, b.c.Now().Add(1*time.Minute)) // TODO: Make configurable
				p.Want(api.PsActive)
				return
			}

			// We are ready to move from Inactive to Dropped, but we have to wait
			// for the placement(s) that are replacing this to become Ready.
			if err := op.MayDrop(p, r); err == nil {
				p.Want(api.PsDropped)
			}

			return

		case api.NsActivating:
			// We've already sent the Activate RPC at least once, and the node
			// is working on it. Just keep waiting. But send another Activate
			// RPC to check whether it's finished and is now Ready. (Or has
			// changed to some other state through crash or bug.)
			//
			// TODO: Extend the lease?
			p.Want(api.PsActive)

		case api.NsDropping:
			// This placement failed to serve too many times. We've given up on it.
			p.Want(api.PsDropped)

		case api.NsActive:
			b.ks.PlacementToState(p, api.PsActive)

		default:
			log.Printf("unexpected remote state: ris=%s, psc=%s", ri.State, p.StateCurrent)
			b.ks.PlacementToState(p, api.PsMissing)
			return
		}

	case api.PsActive:

		ri, ok := n.Get(p.Range().Meta.Ident)
		if !ok {
			// The node doesn't have the placement any more! Abort.
			b.ks.PlacementToState(p, api.PsMissing)
			return
		}

		switch ri.State {
		case api.NsActive:
			// TODO: Extend lease if close to expiry?

			if err := op.MayDeactivate(p, r); err == nil {
				p.Want(api.PsInactive)
			}

		case api.NsDeactivating:
			p.Want(api.PsInactive)

		case api.NsInactive:
			b.ks.PlacementLease(p, time.Time{}) // TODO: Better method
			b.ks.PlacementToState(p, api.PsInactive)

		default:
			log.Printf("unexpected remote state: ris=%s, psc=%s", ri.State, p.StateCurrent)
			b.ks.PlacementToState(p, api.PsMissing)
		}

	case api.PsMissing:
		// This transition only exists to provide an error-handling path to
		// PsDropped without sending any RPCs.
		b.ks.PlacementToState(p, api.PsDropped)
		return

	case api.PsDropped:
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

// initJoin initiates the given join operation, and either sends the resulting
// error down the error channel and closes it, or attaches a callback
//
// Caller must hold the keyspace lock and opJoinsMu.
func (b *Orchestrator) initJoin(opJoin OpJoin) {
	r, err := initJoinInner(b, opJoin)

	// If no error channel is given, this drops the error on the floor.
	if opJoin.Err == nil {
		return
	}

	if err != nil {
		opJoin.Err <- err
		close(opJoin.Err)
		return
	}

	// Unlock operator RPC when the join finishes. This assumes that both
	// parents will become obsolete at once, via Operation.CheckComplete.
	r.OnObsolete(func() {
		close(opJoin.Err)
	})
}

// initJoinInner is a helper func so we can return errors directly. This should
// only be called by initJoin, which plumbs the error into the right channel.
//
// The range returned is not the new range! It's an arbitrary parent range,
// because the caller needs it (to attach the obsolete callback) and this func
// already looks it up from the rID, so I'm being lazy and passing it along.
func initJoinInner(b *Orchestrator, opJoin OpJoin) (*ranje.Range, error) {

	r1, err := b.ks.GetRange(opJoin.Left)
	if err != nil {
		return nil, fmt.Errorf("join with invalid left side: %v (rID=%s)", err, opJoin.Left)
	}

	r2, err := b.ks.GetRange(opJoin.Right)
	if err != nil {
		return nil, fmt.Errorf("join with invalid right side: %v (rID=%s)", err, opJoin.Right)
	}

	constraint := ranje.AnyNode

	// Exclude any node which has a placement of either parent reange.
	// TODO: Make this tweakable once Dest can specify all target nodes.
	for _, r := range []*ranje.Range{r1, r2} {
		for _, p := range r.Placements {
			constraint.Not = append(constraint.Not, p.NodeID)
		}
	}

	// Use replication configs of the left parent for now.
	// TODO: Verify that the two ranges have compatible replication configs, so
	// that the join can be performed without deadlocking.
	n := min(r1.MinPlacements(), r1.TargetActive())
	nIDs := make([]api.NodeID, n)

	// Find the candidates for the placements of the new (joined) range before
	// performing the join. Once that happens, we can't (currently) abort, so
	// the parent ranges will be stuck in RsSubsuming.

	for i := 0; i < n; i++ {

		c := constraint
		if i == 0 && opJoin.Dest != "" {
			c.NodeID = opJoin.Dest
		}

		nIDs[i], err = b.rost.Candidate(nil, c)
		if err != nil {
			return nil, fmt.Errorf("error selecting join candidate: %v", err)
		}

		// Exclude this node from further placements.
		constraint = constraint.WithNot(nIDs[i])
	}

	r3, err := b.ks.JoinTwo(r1, r2)
	if err != nil {
		return nil, fmt.Errorf("join failed: %v (left=%v, right=%v)", err, opJoin.Left, opJoin.Right)
	}

	// If we made it this far, the join has happened and already been persisted.
	// No turning back now.

	for i := range nIDs {
		r3.NewPlacement(nIDs[i])
	}

	// It's not an error that this func returns r1 not r3. The caller needs it.
	// See the docstring.
	return r1, nil
}

// TODO: Dedup this with initJoin, once OnReady is OnObsolete.
func (b *Orchestrator) initSplit(r *ranje.Range, opSplit OpSplit) {
	err := initSplitInner(b, r, opSplit)

	// If no error channel is given, this drops the error on the floor.
	if opSplit.Err == nil {
		return
	}

	if err != nil {
		opSplit.Err <- err
		close(opSplit.Err)
		return
	}

	// If the split was initiated by an operator, then it will have an error
	// channel. When the split is complete (i.e. the range becomes obsolete)
	// close to channel to unblock the RPC handler.
	r.OnObsolete(func() {
		close(opSplit.Err)
	})
}

func initSplitInner(b *Orchestrator, r *ranje.Range, opSplit OpSplit) error {

	// Find candidates for each of the placements in the new the left and right
	// sides *before* performing the split. Once the split happens, we can't
	// (currently) abort, so the parent range will be stuck in RsSubsuming until
	// placement is possible.
	//
	// TODO: Allow split abort by allowing ranges to transition back from
	// RsSubsuming into RsActive, and from RsActive into some new terminal state
	// (RsAborted?) like RsObsolete. Would also need a new entry state
	// (RsNewSplit?) to indicate that it's okay to give up, unlike RsActive.
	// Ranges would need to keep track of how many placements had been created
	// and failed.

	constraint := ranje.AnyNode

	// Exclude any node which has a placement of the parent range.
	// TODO: Make this tweakable; some systems may prefer to split on the parent
	// node (i.e. both sides are placed on the same node as the parent they are
	// split from) and then move.
	for _, p := range r.Placements {
		constraint.Not = append(constraint.Not, p.NodeID)
	}

	n := min(r.MinPlacements(), r.TargetActive()) * 2
	nIDs := make([]api.NodeID, n)
	var err error

	for i := 0; i < n; i += 2 {
		for ii := 0; ii < 2; ii++ {

			// Copy just for this iteration, so we can mutate.
			c := constraint
			if i == 0 {
				if ii == 0 && opSplit.Left != "" {
					c.NodeID = opSplit.Left
				} else if ii == 1 && opSplit.Right != "" {
					c.NodeID = opSplit.Right
				}
			}

			nIDs[i+ii], err = b.rost.Candidate(nil, c)

			// TODO: Make it possible to force a split even when not enough
			// candiates can be found.
			if err != nil {
				return err
			}

			// Exclude this node from further placements.
			constraint = constraint.WithNot(nIDs[i+ii])
		}
	}

	// Perform the actual range split. The source range (r) is moved to
	// RsSubsuming, where it will remain until its placements have all been
	// moved elsewhere. Two new ranges
	rL, rR, err := b.ks.Split(r, opSplit.Key)
	if err != nil {
		return err
	}

	// If we made it this far, the split has happened and already been
	// persisted.

	// TODO: We're creating placements here on a range which is NOT the one
	// we're ticking. That seems... okay? We hold the keyspace lock for the
	// whole tick. But think about edge cases?
	// .
	// We could leave some turd like NextPlacementNodeID on the range and let
	// the first clause (no placements?) in this method pick it up, but (a)
	// that's gross, and (b) what if some later range gets placed on the node
	// before that happens? All worse options.
	// .
	// Actually I think we need to extract this chunk of code up into a "ranges
	// which have splits scheduled" loop before the main all-ranges loop. Join
	// is already up there.

	for i := 0; i < n; i += 2 {
		rL.NewPlacement(nIDs[i])
		rR.NewPlacement(nIDs[i+1])
	}

	return nil
}
