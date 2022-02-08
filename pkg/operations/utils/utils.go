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
	node := rost.NodeByIdent(placement.Addr())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.Addr())
	}

	req, err := rang.GiveRequest(placement)
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
	node := rost.NodeByIdent(placement.Addr())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.Addr())
	}

	err := node.Take(placement)
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

	node := rost.NodeByIdent(placement.Addr())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.Addr())
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

	node := rost.NodeByIdent(placement.Addr())
	if node == nil {
		return fmt.Errorf("no such node: %s", placement.Addr())
	}

	err := node.Serve(placement)
	if err != nil {
		return err
	}

	return nil
}
