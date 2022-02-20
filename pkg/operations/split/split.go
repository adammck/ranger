package split

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
)

//go:generate stringer -type=state

type SplitOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	Done     func(error)
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
	log.Printf("splitting: range=%v, boundary=%q, left=%v, right=%v", op.Range, op.Boundary, op.NodeLeft, op.NodeRight)

	r0, err := op.Keyspace.Get(op.Range)
	if err != nil {
		return fmt.Errorf("can't initiate split; Get failed: %v", err)
	}

	// Moves r into Splitting state
	// TODO: Rename MoveSrc! Clearly it's not just that.
	err = op.Keyspace.DoSplit(r0, op.Boundary)
	if err != nil {
		return fmt.Errorf("can't initiate split; DoSplit failed: %v", err)
	}

	r12 := [2]ranje.Ident{}
	for n, rID := range r0.Children {
		r, err := op.Keyspace.Get(rID)
		if err != nil {
			return fmt.Errorf("can't initiate split; getting child failed: %v", err)
		}

		// Immediately move the new ranges into Placing, so the balancer loop
		// doesn't try to place them in a separate op. (Feels kind of weird to
		// be doing this explicitly while we do so much else implicitly?)
		op.Keyspace.ToState(r, ranje.Placing)

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
	var err error

	op.Keyspace.LogRanges()
	defer op.Keyspace.LogRanges()

	for {
		if err != nil && op.state != Failed {
			panic(fmt.Sprintf("split operation has error while in state: %s", op.state))
		}

		switch op.state {
		case Failed:
			if err == nil {
				panic("split operation in Failed state with no error")
			}
			if op.Done != nil {
				op.Done(err)
			}

			log.Printf("split failed: range=%v, boundary=%q, left=%v, right=%v, err: %v", op.Range, op.Boundary, op.NodeLeft, op.NodeRight, err)
			return

		case Complete:
			if op.Done != nil {
				op.Done(nil)
			}

			log.Printf("split complete: range=%v, boundary=%q, left=%v, right=%v", op.Range, op.Boundary, op.NodeLeft, op.NodeRight)
			return

		case Init:
			panic("split operation re-entered init state")

		case Take:
			s, err = op.take()

		case Give:
			s, err = op.give()

		case Drop:
			s, err = op.drop()

		case Serve:
			s, err = op.serve()
		}

		log.Printf("Split: %s -> %s", op.state, s)
		op.state = s
	}
}

func (op *SplitOp) take() (state, error) {
	r0, err := op.Keyspace.Get(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("split (take) failed: %s", err)
	}

	err = utils.Take(op.Roster, r0.CurrentPlacement)
	if err != nil {
		return Failed, fmt.Errorf("split (take) failed: %s", err)
	}

	return Give, nil
}

func (op *SplitOp) give() (state, error) {
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
			r, err := op.Keyspace.Get(ranges[n])
			if err != nil {
				return fmt.Errorf("Get (%s): %v", sides[n], err)
			}

			p, err := ranje.NewPlacement(r, nodeIDs[n])
			if err != nil {
				return fmt.Errorf("give failed: %v", err)
			}

			err = utils.Give(op.Keyspace, op.Roster, r, p)
			if err != nil {
				return fmt.Errorf("give failed: %v", err)
			}

			// Wait for the placement to become Ready (which it might already be).
			// TODO: This can take a while, so extract FetchWait to a separate step.
			err = p.FetchWait()
			if err != nil {
				// TODO: Provide a more useful error here
				return fmt.Errorf("fetchwait (%s) failed: %s", sides[n], err)
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return Failed, fmt.Errorf("give failed: %s", err)
	}

	return Drop, nil
}

func (op *SplitOp) drop() (state, error) {
	r, err := op.Keyspace.Get(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("split (drop) failed: %s", err)
	}

	err = utils.Drop(op.Roster, r.CurrentPlacement)
	if err != nil {
		return Failed, fmt.Errorf("split (drop) failed: %s", err)
	}

	return Serve, nil
}

func (op *SplitOp) serve() (state, error) {
	sides := [2]string{"left", "right"}
	ranges := []ranje.Ident{op.rL, op.rR}
	nodeIDs := []string{op.NodeLeft, op.NodeRight}

	g, _ := errgroup.WithContext(context.Background())
	for n := range sides {
		n := n

		g.Go(func() error {
			r, err := op.Keyspace.Get(ranges[n])
			if err != nil {
				return fmt.Errorf("Get (%s): %s", sides[n], err)
			}

			p := r.NextPlacement
			if p == nil {
				return fmt.Errorf("NextPlacement (%s) is nil", sides[n])
			}

			nod := op.Roster.NodeByIdent(nodeIDs[n])
			if nod == nil {
				return fmt.Errorf("NodeByIdent (%s) returned no such node: %s", sides[n], nodeIDs[n])
			}

			err = nod.Serve(p)
			if err != nil {
				return fmt.Errorf("serve (%s): %s", sides[n], err)
			}

			err = r.CompleteNextPlacement()
			if err != nil {
				return fmt.Errorf("CompleteNextPlacement (%s): %s", sides[n], err)
			}

			op.Keyspace.ToState(r, ranje.Ready)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		// No state change. Stay in Moving.
		// TODO: Repair the situation somehow.
		return Failed, fmt.Errorf("split (serve) failed: %s", err)
	}

	// TODO: Should this happen via CompletePlacement, too?
	r0, err := op.Keyspace.Get(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("split (Serve) failed: %s", err)
	}

	// This also happens in Range.ChildStateChanged once children are Ready.
	// TODO: Is that a good idea? Here would be more explicit.
	op.Keyspace.ToState(r0, ranje.Obsolete)
	r0.DropPlacement()

	// TODO: This part should probably be handled later by some kind of optional
	//       GC. We won't want to discard the old ranges for systems which need
	//       to reassemble the state from history rather than fetching it from
	//       the current node.
	err = op.Keyspace.Discard(r0)
	if err != nil {
		return Failed, fmt.Errorf("split (discard) failed: %s", err)
	}

	return Complete, nil
}
