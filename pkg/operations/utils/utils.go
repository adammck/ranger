package utils

import (
	"context"
	"fmt"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

const (
	s             = time.Second
	staleTimer    = 10 * s
	giveTimeout   = 3 * s
	takeTimeout   = 3 * s
	untakeTimeout = 3 * s
	dropTimeout   = 3 * s
	serveTimeout  = 3 * s
)

// Utility functions to stop repeating ourselves.
// TODO: These are ironically very repetitive; refactor them.

func ToState(ks *ranje.Keyspace, rID ranje.Ident, state ranje.StateLocal) error {
	r, err := ks.Get(rID)
	if err != nil {
		return err
	}

	err = ks.RangeToState(r, state)
	if err != nil {
		return err
	}

	return nil
}

func Give(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, placement *ranje.Placement) error {
	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	// Perform some last-minute sanity checks.
	// TODO: Is this a good idea? (If so, add them to the other helpers.)
	if p := rang.CurrentPlacement; p != nil {
		if p.State == ranje.SpPending && p == placement {
			panic("giving current placement??")
		}

		if p.State != ranje.SpTaken && p.State != ranje.SpGone {
			return fmt.Errorf("can't give range %s when current placement on node %s is in state %s",
				rang.String(), p.NodeID, p.State)
		}
	}

	// TODO: Rename pb.Placement to something else. It's not a placement!
	parents := map[ranje.Ident]*pb.Placement{}
	addParents(ks, rost, rang, parents)

	req := &pb.GiveRequest{
		Range: rang.Meta.ToProto(),
	}

	for _, p := range parents {
		req.Parents = append(req.Parents, p)
	}

	if placement.State != ranje.SpPending {
		return fmt.Errorf("can't give range %s to node %s when state is %s (wanted SpPending)",
			rang.String(), placement.NodeID, placement.State)
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), giveTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := node.Client.Give(ctx, req)
	if err != nil {
		return err
	}

	// TODO: Use updateLocalState here!
	// TODO: Check the return values of state changes or use MustState!
	rs := roster.RemoteStateFromProto(res.State)
	if rs == roster.StateReady {
		ks.PlacementToState(placement, ranje.SpReady)

	} else if rs == roster.StateFetching {
		ks.PlacementToState(placement, ranje.SpFetching)

	} else if rs == roster.StateFetched {
		// The fetch finished before the client returned.
		ks.PlacementToState(placement, ranje.SpFetching)
		ks.PlacementToState(placement, ranje.SpFetched)

	} else if rs == roster.StateFetchFailed {
		// The fetch failed before the client returned.
		ks.PlacementToState(placement, ranje.SpFetching)
		ks.PlacementToState(placement, ranje.SpFetchFailed)

	} else {
		// Got either Unknown or Taken
		panic(fmt.Sprintf("unexpected remote state from Give: %s", rs.String()))
	}

	return nil
}

func addParents(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, parents map[ranje.Ident]*pb.Placement) {

	// Don't bother serializing the same placement many times. (The range tree
	// won't have cycles, but is also not a DAG.)
	_, ok := parents[rang.Meta.Ident]
	if ok {
		return
	}

	parents[rang.Meta.Ident] = pbPlacement(rost, rang)
	for _, rID := range rang.Parents {
		r, err := ks.Get(rID)
		if err != nil {
			// TODO: Think about how to recover from this. It's bad.
			panic(fmt.Sprintf("getting range with ident %v: %v", rID, err))
		}

		addParents(ks, rost, r, parents)
	}
}

func pbPlacement(rost *roster.Roster, r *ranje.Range) *pb.Placement {

	// Include the address of the node where the range is currently placed, if
	// it's in a state where it can be fetched.
	//
	// TODO: The kv example doesn't care about range history, because it has no
	//       external write log, so can only fetch from nodes. So we can skip
	//       sending them at all. Maybe add a controller feature flag?
	//
	node := ""
	if p := r.CurrentPlacement; p != nil {
		if p.State == ranje.SpReady || p.State == ranje.SpTaken {
			n := rost.NodeByIdent(p.NodeID)
			if n != nil {
				node = n.Addr()
			}
		}
	}

	return &pb.Placement{
		Range: r.Meta.ToProto(),
		Node:  node,
	}
}

// TODO: Remove the Range parameter. Get the relevant stuff from Placement.
// TODO: Plumb in a context from somewhere. Maybe the top controller context.
func Take(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, placement *ranje.Placement) error {

	// TODO: Remove this? The node will enforce it anyway.
	if placement.State != ranje.SpReady {
		return fmt.Errorf("can't take range %s from node %s when state is %s",
			rang.String(), placement.NodeID, placement.State)
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	req := &pb.TakeRequest{
		// lol demeter who?
		Range: rang.Meta.Ident.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), takeTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := node.Client.Take(ctx, req)
	if err != nil {
		// No state change. Placement is still SpReady.
		return err
	}

	return ks.PlacementToState(placement, ranje.SpTaken)
}

func Untake(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, placement *ranje.Placement) error {
	// TODO: Move this into the callers; state is no business of Node.
	if placement.State != ranje.SpTaken {
		return fmt.Errorf("can't untake range %s from node %s when state is %s",
			rang.String(), placement.NodeID, placement.State)
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	req := &pb.UntakeRequest{
		Range: rang.Meta.Ident.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), untakeTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := node.Client.Untake(ctx, req)
	if err != nil {
		// No state change. Placement is still SpTaken.
		return err
	}

	// TODO: Also move this into caller?
	return ks.PlacementToState(placement, ranje.SpReady)
}

// TODO: Can we just take a range here and call+check Placement?
func Drop(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, placement *ranje.Placement) error {
	if placement == nil {
		// This should probably be a panic; how could we possibly have gotten here with a nil placement
		return fmt.Errorf("nil placement")
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	if placement.State != ranje.SpTaken {
		return fmt.Errorf("can't drop range %s from node %s when state is %s",
			rang.String(), placement.NodeID, placement.State)
	}

	req := &pb.DropRequest{
		Range: rang.Meta.Ident.ToProto(),
		Force: false,
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := node.Client.Drop(ctx, req)
	if err != nil {
		// No state change. Placement is still SpTaken.
		return err
	}

	return ks.PlacementToState(placement, ranje.SpDropped)
}

// TODO: Can we just take a range here and call+check Placement?
func Serve(ks *ranje.Keyspace, rost *roster.Roster, rang *ranje.Range, placement *ranje.Placement) error {
	if placement == nil {
		// This should probably be a panic
		return fmt.Errorf("nil placement")
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	if placement.NodeID != node.Remote.Ident {
		return fmt.Errorf("mismatched nodeID: %s != %s",
			placement.NodeID, node.Remote.Ident)
	}

	if placement.State != ranje.SpFetched {
		return fmt.Errorf("can't serve range %s from node %s when state is %s (wanted SpFetched)",
			rang.String(), placement.NodeID, placement.State)
	}

	req := &pb.ServeRequest{
		Range: rang.Meta.Ident.ToProto(),
		Force: false,
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), serveTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := node.Client.Serve(ctx, req)
	if err != nil {
		// No state change. Placement is still SpFetched.
		return err
	}

	return ks.PlacementToState(placement, ranje.SpReady)
}
