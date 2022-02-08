package move

import (
	"fmt"

	"github.com/adammck/ranger/pkg/operations/utils"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type state uint8

const (
	Init state = iota // -> Failed, Taking, Giving
	Failed
	Complete

	Taking       // -> Failed, Giving
	Giving       // -> Failed, FetchWaiting
	FetchWaiting // -> Failed, Serving
	Serving      // -> Failed, Complete
)

type MoveOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	state    state

	// Inputs
	Range ranje.Ident
	Node  string
}

func (op *MoveOp) Run() {
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

func (op *MoveOp) init() state {
	var err error

	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (init) failed: %s\n", err.Error())
		return Failed
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
		return Failed
	}

	return Giving
}

func (op *MoveOp) take() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (take) failed: %s\n", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		fmt.Printf("Move (take) failed: Placement returned nil")
		return Failed
	}

	err = utils.Take(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (take) failed: %s\n", err.Error())
		r.MustState(ranje.Ready) // ???
		return Failed
	}

	return Giving
}

func (op *MoveOp) give() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return Failed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return Failed
	}

	err = utils.Give(op.Roster, r, p)
	if err != nil {
		fmt.Printf("Move (give) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return Failed
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if p.State() == ranje.SpReady {
		r.CompleteNextPlacement()
		return Complete
	}

	return FetchWaiting
}

func (op *MoveOp) fetchWait() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return Failed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		// TODO: wtf to do here? the range is fucked
		return Failed
	}

	err = p.FetchWait()
	if err != nil {
		// TODO: Provide a more useful error here
		fmt.Printf("Move (fetchWait) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return Failed
	}

	return Serving
}

func (op *MoveOp) serve() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return Failed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		return Failed
	}

	err = utils.Serve(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (serve) failed: %s\n", err.Error())

		// TODO: Repair the situation somehow.
		r.MustState(ranje.PlaceError)
		return Failed
	}

	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)

	return Complete
}
