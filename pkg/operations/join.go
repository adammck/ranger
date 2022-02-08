package operations

import (
	"context"
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"golang.org/x/sync/errgroup"
)

type OpJoinState uint8

const (
	OpJoinInit OpJoinState = iota
	OpJoinFailed
	OpJoinComplete
	OpJoinTake
	OpJoinGive
	OpJoinDrop
	OpJoinServe
	OpJoinCleanup
)

type JoinOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	state    OpJoinState

	// Inputs
	RangeSrcLeft  ranje.Ident
	RangeSrcRight ranje.Ident
	NodeDst       string

	// Set by Init after src range is joined.
	// TODO: Better to look up via Range.Child every time?
	rangeDst ranje.Ident
}

func (op *JoinOp) Run() {
	s := op.state

	for {
		switch op.state {
		case OpJoinFailed, OpJoinComplete:
			return

		case OpJoinInit:
			s = op.init()

		case OpJoinTake:
			s = op.take()

		case OpJoinGive:
			s = op.give()

		case OpJoinDrop:
			s = op.drop()

		case OpJoinServe:
			s = op.serve()

		case OpJoinCleanup:
			s = op.cleanup()
		}

		op.state = s
	}
}

func (op *JoinOp) init() OpJoinState {
	r1, err := op.Keyspace.GetByIdent(op.RangeSrcLeft)
	if err != nil {
		fmt.Printf("Join (init, left) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	r2, err := op.Keyspace.GetByIdent(op.RangeSrcRight)
	if err != nil {
		fmt.Printf("Join (init, right) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	// Moves r1 and r2 into Joining state.
	// Starts dest in Pending state. (Like all ranges!)
	// Returns error if either of the ranges aren't ready, or if they're not adjacent.
	r3, err := op.Keyspace.JoinTwo(r1, r2)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return OpJoinFailed
	}

	// ???
	op.rangeDst = r3.Meta.Ident

	fmt.Printf("Joining: %s, %s -> %s\n", r1, r2, r3)
	return OpJoinTake
}

func (op *JoinOp) take() OpJoinState {

	sides := [2]string{"p1", "p2"}
	rangeIDs := []ranje.Ident{op.RangeSrcLeft, op.RangeSrcRight}

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

			err = take(op.Roster, p)
			if err != nil {
				return err
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		fmt.Printf("Join (Take) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	return OpJoinGive
}

func (op *JoinOp) give() OpJoinState {
	err := toState(op.Keyspace, op.rangeDst, ranje.Placing)
	if err != nil {
		fmt.Printf("Join (give) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	r3, err := op.Keyspace.GetByIdent(op.rangeDst)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return OpJoinFailed
	}

	p3, err := ranje.NewPlacement(r3, op.NodeDst)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return OpJoinFailed
	}

	err = give(op.Roster, r3, p3)
	if err != nil {
		fmt.Printf("Join (Give) failed: %s\n", err.Error())
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return OpJoinFailed
	}

	// Wait for the placement to become Ready (which it might already be).
	err = p3.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Join (Fetch) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	return OpJoinDrop
}

func (op *JoinOp) drop() OpJoinState {
	sides := [2]string{"left", "right"}
	rangeIDs := []ranje.Ident{op.RangeSrcLeft, op.RangeSrcRight}

	g, _ := errgroup.WithContext(context.Background())
	for i := range sides {
		s := sides[i]
		rID := rangeIDs[i]

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(rID)
			if err != nil {
				return fmt.Errorf("GetByIdent (%s): %s", s, err.Error())
			}

			err = drop(op.Roster, r.Placement())
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
		return OpJoinFailed
	}

	return OpJoinServe
}

func (op *JoinOp) serve() OpJoinState {
	r, err := op.Keyspace.GetByIdent(op.rangeDst)
	if err != nil {
		fmt.Printf("Join (serve) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	err = serve(op.Roster, r.Placement())
	if err != nil {
		fmt.Printf("Join (serve) failed: %s\n", err.Error())
		return OpJoinFailed
	}

	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)

	return OpJoinCleanup
}

func (op *JoinOp) cleanup() OpJoinState {
	sides := [2]string{"left", "right"}
	rangeIDs := []ranje.Ident{op.RangeSrcLeft, op.RangeSrcRight}

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
		return OpJoinFailed
	}

	return OpJoinComplete
}
