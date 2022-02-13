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
	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
	}

	// Perform some last-minute sanity checks.
	// TODO: Is this a good idea? (If so, add them to the other helpers.)
	if p := rang.Placement(); p != nil {
		if p.State() == ranje.SpPending && p == placement {
			panic("giving current placement??")
		}

		if p.State() != ranje.SpTaken {
			return fmt.Errorf("can't give range %s when current placement on node %s is in state %s",
				rang.String(), p.NodeID(), p.State())
		}
	}

	// Build a list of the ancestors of this range (including itself), and the
	// nodes they're currently placed on where applicable. When moving a range,
	// the first will be the current placement of the exact same range, with the
	// same ident and boundaries. When splitting a range, the first will be the
	// range which this range (either the left or right) was split from, with a
	// different ident and boundaries which are a superset. When joining, the
	// first two will be the ranges which this range were joined from, with
	// different idents and boundaries which are a subset.
	//
	// TODO: There is currently no history pruning, so this will grow forever.
	// TODO: Rename pb.Placement to something else. It's not a placement!
	parents := []*pb.Placement{}
	addParents(rost, rang, &parents)

	req := &pb.GiveRequest{
		Range:   rang.Meta.ToProto(),
		Parents: parents,
	}

	err := node.Give(placement, req)
	if err != nil {
		return err
	}

	return nil
}

func addParents(rost *roster.Roster, rang *ranje.Range, parents *[]*pb.Placement) {
	*parents = append(*parents, pbPlacement(rost, rang))
	for _, rr := range rang.Parents() {
		addParents(rost, rr, parents)
	}
}

func pbPlacement(rost *roster.Roster, r *ranje.Range) *pb.Placement {

	// Include the node where the parent range can currently be found, if
	// it's still placed, such as during a split. Older ranges might not be.
	node := ""
	if p := r.Placement(); p != nil {
		n := rost.NodeByIdent(p.NodeID())
		if n != nil {
			node = n.Addr()
		}
	}

	return &pb.Placement{
		Range: r.Meta.ToProto(),
		Node:  node,
	}
}

func Take(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
	}

	err := node.Take(placement)
	if err != nil {
		return err
	}

	return nil
}

func Untake(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
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

	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
	}

	err := node.Drop(placement)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Can we just take a range here and call+check Placement?
func Serve(rost *roster.Roster, placement *ranje.DurablePlacement) error {
	if placement == nil {
		// This should probably be a panic
		return fmt.Errorf("nil placement")
	}

	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
	}

	err := node.Serve(placement)
	if err != nil {
		return err
	}

	return nil
}
