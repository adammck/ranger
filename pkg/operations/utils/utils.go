package utils

import (
	"fmt"

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
	currNodeAddr := ""

	// Range is currently placed, so look up the node address.
	if p := rang.Placement(); p != nil {
		n := rost.NodeByIdent(p.NodeID())
		if n == nil {
			// The current node is gone. This is bad!
			// But probably not fatal for most systems?
			return fmt.Errorf("current node %s is not found", p.NodeID())
		}
		currNodeAddr = n.Addr()
	}

	node := rost.NodeByIdent(placement.NodeID())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.NodeID())
	}

	req, err := rang.GiveRequest(placement, currNodeAddr)
	if err != nil {
		return err
	}

	err = node.Give(placement, req)
	if err != nil {
		return err
	}

	return nil
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
