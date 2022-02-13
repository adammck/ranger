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

	req, err := giveRequest(rost, rang, placement)
	if err != nil {
		return err
	}

	err = node.Give(placement, req)
	if err != nil {
		return err
	}

	return nil
}

func giveRequest(rost *roster.Roster, rang *ranje.Range, giving *ranje.DurablePlacement) (*pb.GiveRequest, error) {

	// Build a list of the other current placement of this exact range. This
	// doesn't include the ranges which this range was split/joined from! It'll
	// be empty the first time the range is being placed, and have one entry
	// during normal moves.
	parents := []*pb.Placement{}
	if p := rang.Placement(); p != nil {

		// This indicates that the caller is very confused
		if p.State() == ranje.SpPending && p == giving {
			panic("giving current placement??")
		}

		if p.State() != ranje.SpTaken {
			return nil, fmt.Errorf("can't give range %s when current placement on node %s is in state %s",
				rang.String(), p.NodeID(), p.State())
		}

		parents = append(parents, pbPlacement(rost, rang))
	}

	addParents(rost, rang, &parents)

	return &pb.GiveRequest{
		Range:   rang.Meta.ToProto(),
		Parents: parents,
	}, nil
}

func addParents(rost *roster.Roster, r *ranje.Range, parents *[]*pb.Placement) {
	for _, rr := range r.Parents() {
		*parents = append(*parents, pbPlacement(rost, rr))
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
