package join

import (
	"context"
	"fmt"

	"github.com/adammck/ranger/pkg/operations/utils"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"golang.org/x/sync/errgroup"
)

type state uint8

const (
	Init state = iota
	Failed
	Complete
	Take
	Give
	Drop
	Serve
	Cleanup
)

type JoinOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	state    state

	// Inputs
	RangeLeft  ranje.Ident
	RangeRight ranje.Ident
	Node       string

	// Set by Init after src range is joined.
	// TODO: Better to look up via Range.Child every time?
	r ranje.Ident
}

func Run(op *JoinOp) error {
	s, err := op.init()
	if err != nil {
		return err
	}

	op.state = s

	// Run the rest of the operation in the background.
	go op.Go()
	return nil
}

func (op *JoinOp) init() (state, error) {
	r1, err := op.Keyspace.GetByIdent(op.RangeLeft)
	if err != nil {
		fmt.Printf("Join (init, left) failed: %s\n", err.Error())
		return Failed, nil
	}

	r2, err := op.Keyspace.GetByIdent(op.RangeRight)
	if err != nil {
		fmt.Printf("Join (init, right) failed: %s\n", err.Error())
		return Failed, nil
	}

	// Moves r1 and r2 into Joining state.
	// Starts dest in Pending state. (Like all ranges!)
	// Returns error if either of the ranges aren't ready, or if they're not adjacent.
	r3, err := op.Keyspace.JoinTwo(r1, r2)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return Failed, nil
	}

	// ???
	op.r = r3.Meta.Ident

	fmt.Printf("Joining: %s, %s -> %s\n", r1, r2, r3)
	return Take, nil
}

func (op *JoinOp) Go() error {
	s := op.state

	for {
		switch op.state {
		case Failed, Complete:
			return nil

		case Init:
			panic("join operation re-entered init state")

		case Take:
			s = op.take()

		case Give:
			s = op.give()

		case Drop:
			s = op.drop()

		case Serve:
			s = op.serve()

		case Cleanup:
			s = op.cleanup()
		}

		op.state = s
	}
}

func (op *JoinOp) take() state {

	sides := [2]string{"p1", "p2"}
	rangeIDs := []ranje.Ident{op.RangeLeft, op.RangeRight}

	// TODO: Pass the context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	for n := range sides {

		// Keep hold of current values for closure.
		// https://golang.org/doc/faq#closures_and_goroutines
		// TODO: Is this necessary since n is an index?
		s := sides[n]
		rid := rangeIDs[n]

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(rid)
			if err != nil {
				return fmt.Errorf("%s: %s", s, err.Error())
			}

			p := r.NextPlacement()
			if p == nil {
				return fmt.Errorf("%s: NextPlacement returned nil", s)
			}

			err = utils.Take(op.Roster, p)
			if err != nil {
				return err
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		fmt.Printf("Join (Take) failed: %s\n", err.Error())
		return Failed
	}

	return Give
}

func (op *JoinOp) give() state {
	err := utils.ToState(op.Keyspace, op.r, ranje.Placing)
	if err != nil {
		fmt.Printf("Join (give) failed: %s\n", err.Error())
		return Failed
	}

	r3, err := op.Keyspace.GetByIdent(op.r)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return Failed
	}

	p3, err := ranje.NewPlacement(r3, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return Failed
	}

	err = utils.Give(op.Roster, r3, p3)
	if err != nil {
		fmt.Printf("Join (Give) failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return Failed
	}

	// Wait for the placement to become Ready (which it might already be).
	err = p3.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Join (Fetch) failed: %s\n", err.Error())
		return Failed
	}

	return Drop
}

func (op *JoinOp) drop() state {
	sides := [2]string{"left", "right"}
	rangeIDs := []ranje.Ident{op.RangeLeft, op.RangeRight}

	g, _ := errgroup.WithContext(context.Background())
	for i := range sides {
		s := sides[i]
		rID := rangeIDs[i]

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(rID)
			if err != nil {
				return fmt.Errorf("GetByIdent (%s): %s", s, err.Error())
			}

			err = utils.Drop(op.Roster, r.Placement())
			if err != nil {
				return fmt.Errorf("drop (%s): %s", s, err.Error())
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		// No range state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Join (Drop) failed: %s\n", err.Error())
		return Failed
	}

	return Serve
}

func (op *JoinOp) serve() state {
	r, err := op.Keyspace.GetByIdent(op.r)
	if err != nil {
		fmt.Printf("Join (serve) failed: %s\n", err.Error())
		return Failed
	}

	err = utils.Serve(op.Roster, r.Placement())
	if err != nil {
		fmt.Printf("Join (serve) failed: %s\n", err.Error())
		return Failed
	}

	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)

	return Cleanup
}

func (op *JoinOp) cleanup() state {
	sides := [2]string{"left", "right"}
	rangeIDs := []ranje.Ident{op.RangeLeft, op.RangeRight}

	g, _ := errgroup.WithContext(context.Background())
	for i := range sides {
		s := sides[i]
		rID := rangeIDs[i]

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(rID)
			if err != nil {
				return fmt.Errorf("GetByIdent (%s): %s", s, err.Error())
			}

			p := r.Placement()
			if p == nil {
				return fmt.Errorf("%s: NextPlacement returned nil", s)
			}

			p.Forget()

			// TODO: This part should probably be handled later by some kind of GC.
			err = op.Keyspace.Discard(r)
			if err != nil {
				return err

			}

			// This happens implicitly in Range.ChildStateChanged.
			// TODO: Is this a good idea? Here would be more explicit.
			// r.MustState(ranje.Obsolete)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		fmt.Printf("Join (cleanup) failed: %s\n", err.Error())
		return Failed
	}

	return Complete
}
