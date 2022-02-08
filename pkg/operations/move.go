package operations

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type MoveOpState uint8

const (
	OpMoveInit MoveOpState = iota // -> Failed, Taking, Giving
	OpMoveFailed
	OpMoveComplete

	OpMoveTaking       // -> Failed, Giving
	OpMoveGiving       // -> Failed, FetchWaiting
	OpMoveFetchWaiting // -> Failed, Serving
	OpMoveServing      // -> Failed, Complete
)

type MoveOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	state    MoveOpState

	// Inputs
	Range ranje.Ident
	Node  string
}

func (op *MoveOp) Run() {
	s := op.state

	for {
		switch s {
		case OpMoveFailed, OpMoveComplete:
			return

		case OpMoveInit:
			s = op.init()

		case OpMoveTaking:
			s = op.take()

		case OpMoveGiving:
			s = op.give()

		case OpMoveFetchWaiting:
			s = op.fetchWait()

		case OpMoveServing:
			s = op.serve()
		}

		op.state = s
	}
}

// In order to be robust against interruption, each of these steps must be
// idempotent! Remember that we may crash at any line.

func (op *MoveOp) init() MoveOpState {
	var err error

	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (init) failed: %s\n", err.Error())
		return OpMoveFailed
	}

	// If the range is currently ready, it's placed on some node.
	// TODO: Now that we have MoveOpState, do we even need a special range state
	// to indicates that it's moving? Perhaps we can unify the op states into a
	// single 'some op is happening' state on the range.
	if r.State() == ranje.Ready {
		r.MustState(ranje.Moving)

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		// Not ready, but still eligible to be placed. (This isn't necessarily
		// an error state. All ranges are pending when created.)
		r.MustState(ranje.Placing)

	} else {
		fmt.Printf("unexpected range state?! %s\n", r.State())
		return OpMoveFailed
	}

	return OpMoveGiving
}

func (op *MoveOp) take() MoveOpState {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (take) failed: %s\n", err.Error())
		return OpMoveFailed
	}

	p := r.Placement()
	if p == nil {
		fmt.Printf("Move (take) failed: Placement returned nil")
		return OpMoveFailed
	}

	err = take(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (take) failed: %s\n", err.Error())
		r.MustState(ranje.Ready) // ???
		return OpMoveFailed
	}

	return OpMoveGiving
}

func (op *MoveOp) give() MoveOpState {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return OpMoveFailed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return OpMoveFailed
	}

	err = give(op.Roster, r, p)
	if err != nil {
		fmt.Printf("Move (give) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return OpMoveFailed
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if p.State() == ranje.SpReady {
		r.CompleteNextPlacement()
		return OpMoveComplete
	}

	return OpMoveFetchWaiting
}

func (op *MoveOp) fetchWait() MoveOpState {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return OpMoveFailed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return OpMoveFailed
	}

	err = p.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Move (fetchWait) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return OpMoveFailed
	}

	return OpMoveServing
}

func (op *MoveOp) serve() MoveOpState {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return OpMoveFailed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		return OpMoveFailed
	}

	err = serve(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (serve) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return OpMoveFailed
	}

	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)

	return OpMoveComplete
}
