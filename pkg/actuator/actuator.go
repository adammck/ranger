package actuator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
)

type Actuator struct {
	ks  *keyspace.Keyspace
	ros *roster.Roster

	// TODO: Move this into the Tick method; just pass it along.
	rpcWG sync.WaitGroup

	// RPCs which have been sent (via orch.RPC) but not yet completed. Used to
	// avoid sending the same RPC redundantly every single tick. (Though RPCs
	// *can* be re-sent an arbitrary number of times. Rangelet will dedup them.)
	inFlight   map[string]struct{}
	inFlightMu sync.Mutex
}

const rpcTimeout = 1 * time.Second

func New(ks *keyspace.Keyspace, ros *roster.Roster) *Actuator {
	return &Actuator{
		ks:       ks,
		ros:      ros,
		inFlight: map[string]struct{}{},
	}
}

func (a *Actuator) rpc(p *ranje.Placement, method string, f func()) {
	key := fmt.Sprintf("%s:%s:%s", p.NodeID, p.Range().Meta.Ident, method)

	a.inFlightMu.Lock()
	_, ok := a.inFlight[key]
	if !ok {
		a.inFlight[key] = struct{}{}
	}
	a.inFlightMu.Unlock()

	if ok {
		log.Printf("dropping in-flight RPC: %s", key)
		return
	}

	a.rpcWG.Add(1)

	go func() {

		// TODO: Inject some client-side chaos here, too. RPCs complete very
		//       quickly locally, which doesn't test our in-flight thing well.
		f()

		a.inFlightMu.Lock()
		_, ok := a.inFlight[key]
		if !ok {
			// Critical this works, because could drop all RPCs.
			panic(fmt.Sprintf("no record of in-flight RPC: %s", key))
		}
		log.Printf("RPC completed: %s", key)
		delete(a.inFlight, key)
		a.inFlightMu.Unlock()

		a.rpcWG.Done()
	}()
}

func (a *Actuator) WaitRPCs() {
	a.rpcWG.Wait()
}

func (a *Actuator) Give(p *ranje.Placement, n *roster.Node) {
	a.rpc(p, "Give", func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		err := give(ctx, n, p, getParents(a.ks, a.ros, p.Range()))
		if err != nil {
			log.Printf("error giving %v to %s: %v", p.LogString(), n.Ident(), err)
		}
	})
}

func give(ctx context.Context, n *roster.Node, p *ranje.Placement, parents []*pb.Parent) error {
	log.Printf("giving %s to %s...", p.LogString(), n.Ident())

	req := &pb.GiveRequest{
		Range:   p.Range().Meta.ToProto(),
		Parents: parents,
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Give(ctx, req)
	if err != nil {
		log.Printf("error giving %s to %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	// Parse the response, which contains the current state of the range.
	// TODO: This should only contain the remote state. We already know the
	//       meta, and the rest (usage info) is all probably zero at this point,
	//       and can be filled in by the next probe anyway.
	info, err := info.RangeInfoFromProto(res.RangeInfo)
	if err != nil {
		return fmt.Errorf("malformed probe response from %v: %v", n.Remote.Ident, err)
	}

	// Update the range info cache on the Node. This is faster than waiting for
	// the next probe, but is otherwise the same thing.
	n.UpdateRangeInfo(&info)

	log.Printf("gave %s to %s; info=%v", p.LogString(), n.Ident(), info)
	return nil
}

func (a *Actuator) Serve(p *ranje.Placement, n *roster.Node) {
	a.rpc(p, "Serve", func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		err := serve(ctx, n, p)
		if err != nil {
			log.Printf("error serving %v to %s: %v", p.LogString(), n.Ident(), err)
		}
	})
}

func serve(ctx context.Context, n *roster.Node, p *ranje.Placement) error {
	log.Printf("serving %s to %s...", p.LogString(), n.Ident())
	rID := p.Range().Meta.Ident

	// TODO: Include range parents
	req := &pb.ServeRequest{
		Range: rID.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Serve(ctx, req)
	if err != nil {
		log.Printf("error serving %s to %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	s := state.RemoteStateFromProto(res.State)

	// Update the state in the range cache.
	err = n.UpdateRangeState(rID, s)
	if err != nil {
		// Don't propagate this error, because the node *did* accept the
		// command, it's just our local state which is somehow screwed up.
		log.Printf("error updating range state: %v", err)
		return nil
	}

	log.Printf("served %s to %s; state=%v", p.LogString(), n.Ident(), s)
	return nil
}

func (a *Actuator) Take(p *ranje.Placement, n *roster.Node) {
	a.rpc(p, "Take", func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		err := take(ctx, n, p)
		if err != nil {
			log.Printf("error taking %v from %s: %v", p.LogString(), n.Ident(), err)
		}
	})
}

func take(ctx context.Context, n *roster.Node, p *ranje.Placement) error {
	log.Printf("deactivating %s from %s...", p.LogString(), n.Ident())
	rID := p.Range().Meta.Ident

	// TODO: Include range parents
	req := &pb.TakeRequest{
		Range: rID.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Take(ctx, req)
	if err != nil {
		log.Printf("error deactivating %s from %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	s := state.RemoteStateFromProto(res.State)

	// Update the state in the range cache.
	err = n.UpdateRangeState(rID, s)
	if err != nil {
		// Don't propagate this error, because the node *did* accept the
		// command, it's just our local state which is somehow screwed up.
		log.Printf("error updating range state: %v", err)
		return nil
	}

	log.Printf("took %s from %s; state=%v", p.LogString(), n.Ident(), s)
	return nil
}

func (a *Actuator) Drop(p *ranje.Placement, n *roster.Node) {
	a.rpc(p, "Drop", func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		err := drop(ctx, n, p)
		if err != nil {
			log.Printf("error dropping %v from %s: %v", p.LogString(), n.Ident(), err)
		}
	})
}

func drop(ctx context.Context, n *roster.Node, p *ranje.Placement) error {
	log.Printf("dropping %s from %s...", p.LogString(), n.Ident())
	rID := p.Range().Meta.Ident

	// TODO: Include range parents
	req := &pb.DropRequest{
		Range: rID.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Drop(ctx, req)
	if err != nil {
		log.Printf("error dropping %s from %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	s := state.RemoteStateFromProto(res.State)

	// Update or drop the state in the range cache.
	err = n.UpdateRangeState(rID, s)
	if err != nil {
		// Don't propagate this error, because the node *did* accept the
		// command, it's just our local state which is somehow screwed up.
		log.Printf("error updating range state: %v", err)
		return nil
	}

	log.Printf("dropped %s from %s; state=%v", p.LogString(), n.Ident(), s)
	return nil
}

func getParents(ks *keyspace.Keyspace, rost *roster.Roster, rang *ranje.Range) []*pb.Parent {
	parents := []*pb.Parent{}
	seen := map[ranje.Ident]struct{}{}
	addParents(ks, rost, rang, &parents, seen)
	return parents
}

func addParents(ks *keyspace.Keyspace, rost *roster.Roster, rang *ranje.Range, parents *[]*pb.Parent, seen map[ranje.Ident]struct{}) {

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
