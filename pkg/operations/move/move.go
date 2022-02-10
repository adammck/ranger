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
	Giving       // -> Failed, Untaking, FetchWaiting
	Untaking     // -> Failed
	FetchWaiting // -> Failed, Serving
	Serving      // -> Failed, Complete, Dropping
	Dropping     // -> Failed, Complete
)

type MoveOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	state    state

	// Inputs
	Range ranje.Ident
	Node  string
}

func Run(op *MoveOp) error {
	s, err := op.init()
	if err != nil {
		return err
	}

	op.state = s

	// Run the rest of the operation in the background.
	go op.Go()
	return nil
}

// This is run synchronously, to determine whether the operation can proceed. If
// so, the rest of the operation is run in a goroutine.
func (op *MoveOp) init() (state, error) {
	var err error

	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("can't initiate move: %v", err)
	}

	// If the range is currently ready, it's placed on some node.
	// TODO: Now that we have MoveOpState, do we even need a special range state
	// to indicates that it's moving? Perhaps we can unify the op states into a
	// single 'some op is happening' state on the range.
	if r.State() == ranje.Ready {

		// TODO: Sanity check here that we're not trying to move the range to
		// the node it's already on. The operation fails gracefully even if we
		// do try to do this, but involves a brief unavailability because it
		// will Take, then try to Give (and fail), then Untake.

		r.MustState(ranje.Moving)
		return Taking, nil

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		// Not ready, but still eligible to be placed. (This isn't necessarily
		// an error state. All ranges are pending when created.)
		r.MustState(ranje.Placing)
		return Giving, nil

	} else {
		return Failed, fmt.Errorf("can't initiate move of range in state %q", r.State())
	}
}

func (op *MoveOp) Go() {
	s := op.state

	for {
		switch s {
		case Failed, Complete:
			return

		case Init:
			panic("move operation re-entered init state")

		case Taking:
			s = op.take()

		case Giving:
			s = op.give()

		case Untaking:
			s = op.untake()

		case FetchWaiting:
			s = op.fetchWait()

		case Serving:
			s = op.serve()

		case Dropping:
			s = op.drop()
		}

		fmt.Printf("Move: %d -> %d\n", op.state, s)
		op.state = s
	}
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
		fmt.Printf("Move (give) failed: %s\n", err.Error())
		return Failed
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		fmt.Printf("Move (give) failed: %s\n", err.Error())
		return Failed
	}

	err = utils.Give(op.Roster, r, p)
	if err != nil {
		fmt.Printf("Move (give) failed: %s\n", err.Error())

		// Clean up p. No return value.
		r.ClearNextPlacement()

		switch r.State() {
		case ranje.Placing:
			// During initial placement, we can just fail without cleanup. The
			// range is still not assigned. The balancer should retry the
			// placement, perhaps on a different node.
			r.MustState(ranje.PlaceError)

		case ranje.Moving:
			// When moving, we have already taken the range from the src node,
			// but failed to give it to the dest! We must untake it from the
			// src, to avoid failing in a state where nobody has the range.
			return Untaking

		default:
			panic(fmt.Sprintf("impossible range state: %s", r.State()))
		}

		return Failed
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if p.State() == ranje.SpReady {
		return complete(r)
	}

	return FetchWaiting
}

func (op *MoveOp) untake() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (untake) failed: %s\n", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		fmt.Printf("Move (untake) failed: Placement returned nil")
		return Failed
	}

	err = utils.Untake(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (untake) failed: %s\n", err.Error())
		return Failed // TODO: Try again?!
	}

	// The range is now ready again, because the current placement is ready.
	// (and the next placement is gone.)
	r.MustState(ranje.Ready)

	// Always transition into failed, because even though this step succeeded
	// and service has been restored to src, the move was a failure.
	return Failed
}

func (op *MoveOp) fetchWait() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (fetchWait) failed: %s\n", err.Error())
		return Failed
	}

	p := r.NextPlacement()
	if p == nil {
		fmt.Printf("Move (fetchWait) failed: NextPlacement is nil")
		return Failed
	}

	err = p.FetchWait()
	if err != nil {
		fmt.Printf("Move (fetchWait) failed: %s\n", err.Error())
		return Failed
	}

	return Serving
}

func (op *MoveOp) serve() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (serve) failed: %s\n", err.Error())
		return Failed
	}

	p := r.NextPlacement()
	if p == nil {
		fmt.Printf("Move (serve) failed: NextPlacement is nil")
		return Failed
	}

	err = utils.Serve(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (serve) failed: %s\n", err.Error())
		return Failed
	}

	switch r.State() {
	case ranje.Moving:
		// This is a range move, so even though the next placement is ready to
		// serve, we still have to clean up the current placement. We could mark
		// the range as ready now, to minimize the not-ready window, but a drop
		// operation should be fast, and it would be weird.
		return Dropping

	case ranje.Placing:
		// This is an initial placement, so we have no previous node to drop
		// data from. We're done.
		return complete(r)

	default:
		panic(fmt.Sprintf("impossible range state: %s", r.State()))
	}
}

func (op *MoveOp) drop() state {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		fmt.Printf("Move (drop) failed: %s\n", err.Error())
		return Failed
	}

	p := r.Placement()
	if p == nil {
		fmt.Printf("Move (drop) failed: Placement is nil")
		return Failed
	}

	err = utils.Drop(op.Roster, p)
	if err != nil {
		fmt.Printf("Move (drop) failed: %s\n", err.Error())
		return Failed
	}

	return complete(r)
}

func complete(r *ranje.Range) state {
	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)
	return Complete
}
