package join

import (
	"context"
	"fmt"
	"log"

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

//go:generate stringer -type=state

type JoinOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	Done     func(error)
	state    state

	// Inputs
	RangeLeft  ranje.Ident
	RangeRight ranje.Ident
	Node       string

	// Set by Init after src range is joined.
	// TODO: Better to look up via Range.Child every time?
	r ranje.Ident
}

func (op *JoinOp) Init() error {
	log.Printf("joining: left=%v, right=%v, node=%v", op.RangeLeft, op.RangeRight, op.Node)

	r1, err := op.Keyspace.GetByIdent(op.RangeLeft)
	if err != nil {
		return fmt.Errorf("can't initiate join; GetByIdent(left) failed: %v", err)
	}

	r2, err := op.Keyspace.GetByIdent(op.RangeRight)
	if err != nil {
		return fmt.Errorf("can't initiate join; GetByIdent(right) failed: %v", err)
	}

	// Moves r1 and r2 into Joining state.
	// Starts dest in Pending state. (Like all ranges!)
	// Returns error if either of the ranges aren't ready, or if they're not adjacent.
	r3, err := op.Keyspace.JoinTwo(r1, r2)
	if err != nil {
		return fmt.Errorf("can't initiate join; JoinTwo failed: %v", err)
	}

	// TODO: Get rid of this; do the lookup every time.
	op.r = r3.Meta.Ident

	op.state = Take

	return nil
}

func (op *JoinOp) Run() {
	s := op.state
	var err error

	op.Keyspace.LogRanges()
	defer op.Keyspace.LogRanges()

	for {
		switch op.state {
		case Failed:
			if err == nil {
				panic("join operation in Failed state with no error")
			}
			if op.Done != nil {
				op.Done(err)
			}

			log.Printf("join failed: left=%v, right=%v, node=%v, err: %v", op.RangeLeft, op.RangeRight, op.Node, err)
			return

		case Complete:
			if op.Done != nil {
				op.Done(nil)
			}

			log.Printf("join complete: left=%v, right=%v, node=%v", op.RangeLeft, op.RangeRight, op.Node)
			return

		case Init:
			panic("join operation re-entered init state")

		case Take:
			s, err = op.take()

		case Give:
			s, err = op.give()

		case Drop:
			s, err = op.drop()

		case Serve:
			s, err = op.serve()

		case Cleanup:
			s, err = op.cleanup()
		}

		log.Printf("Join: %s -> %s", op.state, s)
		op.state = s
	}
}

func (op *JoinOp) take() (state, error) {

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

			p := r.CurrentPlacement
			if p == nil {
				return fmt.Errorf("%s: CurrentPlacement is nil", s)
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
		return Failed, fmt.Errorf("take failed: %v", err)
	}

	return Give, nil
}

func (op *JoinOp) give() (state, error) {
	err := utils.ToState(op.Keyspace, op.r, ranje.Placing)
	if err != nil {
		return Failed, fmt.Errorf("give failed: ToState: %v", err)
	}

	r3, err := op.Keyspace.GetByIdent(op.r)
	if err != nil {
		return Failed, fmt.Errorf("give failed: GetByIdent: %v", err)
	}

	p3, err := ranje.NewPlacement(r3, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return Failed, fmt.Errorf("give failed: NewPlacement: %v", err)
	}

	err = utils.Give(op.Roster, r3, p3)
	if err != nil {
		// This is a bad situation; the range has been taken from the src, but
		// can't be given to the dest! So we stay in Moving forever.
		// TODO: Repair the situation somehow.
		//r.MustState(ranje.MoveError)
		return Failed, fmt.Errorf("give failed: utils.Give: %v", err)
	}

	// Wait for the placement to become Ready (which it might already be).
	err = p3.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		return Failed, fmt.Errorf("give failed: FetchWait: %v", err)
	}

	// TODO: Shouldn't this be Serve, before Drop?
	return Drop, nil
}

func (op *JoinOp) drop() (state, error) {
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

			err = utils.Drop(op.Roster, r.CurrentPlacement)
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
		return Failed, fmt.Errorf("drop (Wait) failed: %s", err)
	}

	return Serve, nil
}

func (op *JoinOp) serve() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.r)
	if err != nil {
		return Failed, fmt.Errorf("serve (GetByIdent) failed: %s", err)
	}

	p := r.NextPlacement
	if p == nil {
		return Failed, fmt.Errorf("serve: NextPlacement is nil")
	}

	err = utils.Serve(op.Roster, p)
	if err != nil {
		return Failed, fmt.Errorf("serve (utils.Serve) failed: %s", err)
	}

	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)

	return Cleanup, nil
}

func (op *JoinOp) cleanup() (state, error) {
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

			p := r.CurrentPlacement
			if p == nil {
				return fmt.Errorf("%s: NextPlacement returned nil", s)
			}

			// This also happens implicitly in Range.ChildStateChanged.
			// TODO: Is this a good idea? Here would be more explicit.
			r.MustState(ranje.Obsolete)
			r.DropPlacement()

			// TODO: This part should probably be handled later by some kind of
			//       optional GC. We won't want to discard the old ranges for
			//       systems which need to reassemble the state from history
			//       rather than fetching it from the current node.
			err = op.Keyspace.Discard(r)
			if err != nil {
				return err
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return Failed, fmt.Errorf("cleanup (Wait) failed: %s", err)
	}

	return Complete, nil
}
