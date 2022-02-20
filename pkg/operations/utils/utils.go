package utils

import (
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

// Utility functions to stop repeating ourselves.
// TODO: These are ironically very repetitive; refactor them.

func ToState(ks *ranje.Keyspace, rID ranje.Ident, state ranje.StateLocal) error {
	r, err := ks.GetByIdent(rID)
	if err != nil {
		return err
	}

	err = r.ToState(state)
	if err != nil {
		return err
	}

	return nil
}

func Give(rost *roster.Roster, rang *ranje.Range, placement *ranje.DurablePlacement) error {
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

		if p.State != ranje.SpTaken {
			return fmt.Errorf("can't give range %s when current placement on node %s is in state %s",
				rang.String(), p.NodeID, p.State)
		}
	}

	// TODO: Rename pb.Placement to something else. It's not a placement!
	parents := map[ranje.Ident]*pb.Placement{}
	addParents(rost, rang, parents)

	req := &pb.GiveRequest{
		Range: rang.Meta.ToProto(),
	}

	for _, p := range parents {
		req.Parents = append(req.Parents, p)
	}

	err := node.Give(placement, req)
	if err != nil {
		return err
	}

	return nil
}

func addParents(rost *roster.Roster, rang *ranje.Range, parents map[ranje.Ident]*pb.Placement) {

	// Don't bother serializing the same placement many times. (The range tree
	// won't have cycles, but is also not a DAG.)
	_, ok := parents[rang.Meta.Ident]
	if ok {
		return
	}

	parents[rang.Meta.Ident] = pbPlacement(rost, rang)
	for _, rr := range rang.Parents() {
		addParents(rost, rr, parents)
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

func Take(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	err := node.Take(placement)
	if err != nil {
		return err
	}

	return nil
}

func Untake(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	err := node.Untake(placement)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Can we just take a range here and call+check Placement?
func Drop(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	if placement == nil {
		// This should probably be a panic; how could we possibly have gotten here with a nil placement
		return fmt.Errorf("nil placement")
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	err := node.Drop(placement)
	if err != nil {
		return err
	}

	// TODO: Move state changes out of Node, put them here.
	//placement.ToState(ranje.SpDropped)

	return nil
}

// TODO: Can we just take a range here and call+check Placement?
func Serve(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	if placement == nil {
		// This should probably be a panic
		return fmt.Errorf("nil placement")
	}

	node := rost.NodeByIdent(placement.NodeID)
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID)
	}

	err := node.Serve(placement)
	if err != nil {
		return err
	}

	return nil
}
