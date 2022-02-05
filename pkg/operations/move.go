package operations

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
)

// this is an attempt at a resumable operation, so is weirder than the other two

type MoveOpState uint8

const (
	Init MoveOpState = iota // -> Failed, Taking, Giving
	Failed
	Complete

	Taking       // -> Failed, Giving
	Giving       // -> Failed, FetchWaiting
	FetchWaiting // -> Failed, Serving
	Serving      // -> Failed, Complete
)

type moveOp struct {
	state MoveOpState

	// inputs; don't touch these after init!
	r    *ranje.Range
	node *ranje.Node

	// other state; needs persisting, but how can we persist these bloody pointers? store kind of idents and then look them up in every step.
	src  *ranje.Placement
	dest *ranje.Placement
}

func Move(r *ranje.Range, node *ranje.Node) {
	op := moveOp{r: r, node: node}
	s := op.state

	for {
		switch s {
		case Failed, Complete:
			return

		case Init:
			s = op.init()

		case Taking:
			s = op.take()

		case Giving:
			s = op.give()

		case FetchWaiting:
			s = op.fetchWait()

		case Serving:
			s = op.serve()
		}

		op.state = s
	}
}

// In order to be robust against interruption, each of these steps must be
// idempotent! Remember that we may crash at any line.

func (op *moveOp) init() MoveOpState {
	var err error

	op.dest, err = ranje.NewPlacement(op.r, op.node)
	if err != nil {
		fmt.Printf("Move failed: error creating placement: %s\n", err.Error())
		op.r.MustState(ranje.PlaceError)
		return Failed
	}

	// If the range is currently ready, it's placed on some node.
	// TODO: Now that we have MoveOpState, do we even need a special range state
	// to indicates that it's moving? Perhaps we can unify the op states into a
	// single 'some op is happening' state on the range.
	if op.r.State() == ranje.Ready {
		op.r.MustState(ranje.Moving)

	} else if op.r.State() == ranje.Quarantined || op.r.State() == ranje.Pending {
		// Not ready, but still eligible to be placed. (This isn't necessarily
		// an error state. All ranges are pending when created.)
		op.r.MustState(ranje.Placing)

	} else {
		// TODO: Don't panic! The range is probably already being moved.
		panic(fmt.Sprintf("unexpectd range state?! %s", op.r.State()))
		return Failed
	}

	op.src = op.r.Placement()
	if op.src != nil {
		return Taking
	}

	return Giving
}

func (op *moveOp) take() MoveOpState {
	err := op.src.Take()
	if err != nil {
		fmt.Printf("Take failed: %s\n", err.Error())
		op.r.MustState(ranje.Ready) // ???
		return Failed
	}

	return Giving
}

func (op *moveOp) give() MoveOpState {
	pState, err := op.dest.Give()
	if err != nil {
		fmt.Printf("Give failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		op.r.MustState(ranje.PlaceError)
		return Failed
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if pState == ranje.SpReady {
		op.r.CompleteNextPlacement()
		return Complete
	}

	return FetchWaiting
}

func (op *moveOp) fetchWait() MoveOpState {
	err := op.dest.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Fetch failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		op.r.MustState(ranje.PlaceError)
		return Failed
	}

	return Serving
}

func (op *moveOp) serve() MoveOpState {
	err := op.dest.Serve()
	if err != nil {
		fmt.Printf("Serve failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		op.r.MustState(ranje.PlaceError)
		return Failed
	}

	op.r.CompleteNextPlacement()
	op.r.MustState(ranje.Ready)

	return Complete
}
