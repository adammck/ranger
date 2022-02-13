package split

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
)

type SplitOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	Done     func()
	state    state

	// Inputs
	Range     ranje.Ident
	Boundary  ranje.Key
	NodeLeft  string
	NodeRight string

	// Set by Init after src range is split.
	// TODO: Better to look up via Range.Child every time?
	rL ranje.Ident
	rR ranje.Ident
}

func (op *SplitOp) Init() error {
	r0, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return fmt.Errorf("can't initiate split; GetByIdent failed: %v", err)
	}

	// Moves r into Splitting state
	// TODO: Rename MoveSrc! Clearly it's not just that.
	err = op.Keyspace.DoSplit(r0, op.Boundary)
	if err != nil {
		return fmt.Errorf("can't initiate split; DoSplit failed: %v", err)
	}

	r12 := [2]ranje.Ident{}
	for n := range r12 {
		r, err := r0.Child(n)
		if err != nil {
			return fmt.Errorf("can't initiate split; r0.Child(%d) failed: %v", n, err)
		}

		r12[n] = r.Meta.Ident
	}

	// Store these (by ident) in the op for later.
	// TODO: Get rid of this; do the lookup every time.
	op.rL = r12[0]
	op.rR = r12[1]

	op.state = Take
	return nil
}

func (op *SplitOp) Run() {
	s := op.state

	for {
		switch op.state {
		case Failed, Complete:
			if op.Done != nil {
				op.Done() // TODO: Send an error?
			}
			return

		case Init:
			panic("split operation re-entered init state")

		case Take:
			s = op.take()

		case Give:
			s = op.give()

		case Drop:
			s = op.drop()

		case Serve:
			s = op.serve()
		}

		fmt.Printf("Split: %d -> %d\n", op.state, s)
		op.state = s
	}
}

func (op *SplitOp) take() state {
	r0, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Split (Take) failed: %s\n", err.Error())
		return Failed
	}

	err = utils.Take(op.Roster, r0.Placement())
	if err != nil {
		fmt.Printf("Split (Take) failed: %s\n", err.Error())
		return Failed
	}

	return Give
}

func (op *SplitOp) give() state {
	// TODO: Group these together in some ephemeral type?
	sides := [2]string{"left", "right"}
	ranges := []ranje.Ident{op.rL, op.rR}
	nodeIDs := []string{op.NodeLeft, op.NodeRight}

	// TODO: Pass a context into Take, to cancel both together.
	g, _ := errgroup.WithContext(context.Background())
	for n := range sides {

		// Keep hold of current values for closure.
		// https://golang.org/doc/faq#closures_and_goroutines
		// TODO: Is this necessary since n is an index?
		n := n

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(ranges[n])
			if err != nil {
				return fmt.Errorf("GetByIdent (%s): %s", sides[n], err.Error())
			}

			p := r.NextPlacement()
			if p == nil {
				return fmt.Errorf("NextPlacement (%s) returned nil", sides[n])
			}

			req, err := r.GiveRequest(p, "") // TODO: currNodeAddr!
			if err != nil {
				return fmt.Errorf("GiveRequest (%s) failed: %s", sides[n], err.Error())
			}

			nod := op.Roster.NodeByIdent(nodeIDs[n])
			if nod == nil {
				return fmt.Errorf("NodeByIdent(%s) returned no such node: %s", sides[n], nodeIDs[n])
			}

			// TODO: This doesn't work yet! Give doesn't include parents info.
			err = nod.Give(p, req)
			if err != nil {
				return fmt.Errorf("give (%s) failed: %s", sides[n], err.Error())
			}

			// Wait for the placement to become Ready (which it might already be).
			// TODO: This can take a while, so extract FetchWait to a separate step.
			err = p.FetchWait()
			if err != nil {
				// TODO: Provide a more useful error here
				return fmt.Errorf("fetchwait (%s) failed: %s", sides[n], err.Error())
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())
		return Failed
	}

	return Drop
}

func (op *SplitOp) drop() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Split (Drop) failed: %s\n", err.Error())
		return Failed
	}

	err = utils.Drop(op.Roster, r.Placement())
	if err != nil {
		fmt.Printf("Split (Drop) failed: %s\n", err.Error())
		return Failed
	}

	return Serve
}

func (op *SplitOp) serve() state {
	sides := [2]string{"left", "right"}
	ranges := []ranje.Ident{op.rL, op.rR}
	nodeIDs := []string{op.NodeLeft, op.NodeRight}

	g, _ := errgroup.WithContext(context.Background())
	for n := range sides {
		n := n

		g.Go(func() error {
			r, err := op.Keyspace.GetByIdent(ranges[n])
			if err != nil {
				return fmt.Errorf("GetByIdent (%s): %s", sides[n], err.Error())
			}

			p := r.NextPlacement()
			if p == nil {
				return fmt.Errorf("NextPlacement (%s) returned nil", sides[n])
			}

			nod := op.Roster.NodeByIdent(nodeIDs[n])
			if nod == nil {
				return fmt.Errorf("NodeByIdent (%s) returned no such node: %s", sides[n], nodeIDs[n])
			}

			err = nod.Serve(p)
			if err != nil {
				return fmt.Errorf("serve (%s): %s", sides[n], err.Error())
			}

			err = r.CompleteNextPlacement()
			if err != nil {
				return fmt.Errorf("CompleteNextPlacement (%s): %s", sides[n], err.Error())
			}

			r.MustState(ranje.Ready)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		fmt.Printf("Serve (Drop) failed: %s\n", err.Error())
		return Failed
	}

	// TODO: Should this happen via CompletePlacement, too?
	r0, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Split (Serve) failed: %s\n", err.Error())
		return Failed
	}
	r0.Placement().Forget()

	// This happens in Range.ChildStateChanged once children are Ready.
	// TODO: Is that a good idea? Here would be more explicit.
	// r.MustState(ranje.Obsolete)

	// TODO: Move this to some background GC routine in the balancer.
	err = op.Keyspace.Discard(r0)
	if err != nil {
		fmt.Printf("Discard failed: %s\n", err.Error())
	}

	return Complete
}