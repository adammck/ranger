package move

import (
	"fmt"
	"log"

	"github.com/adammck/ranger/pkg/operations/utils"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type state uint8

const (
	Init state = iota
	Failed
	Complete
	Take
	Give
	Untake
	FetchWait
	Serve
	Drop
)

//go:generate stringer -type=state

type MoveOp struct {
	Keyspace *ranje.Keyspace
	Roster   *roster.Roster
	Done     func(error)
	state    state

	// Inputs
	Range ranje.Ident
	Node  string
}

// This is run synchronously, to determine whether the operation can proceed. If
// so, the rest of the operation is run in a goroutine.
func (op *MoveOp) Init() error {
	log.Printf("moving: range=%v, node=%v", op.Range, op.Node)
	var err error

	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return fmt.Errorf("can't initiate move; GetByIdent failed: %v", err)
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
		op.state = Take
		return nil

	} else if r.State() == ranje.Quarantined || r.State() == ranje.Pending {
		// Not ready, but still eligible to be placed. (This isn't necessarily
		// an error state. All ranges are pending when created.)
		r.MustState(ranje.Placing)
		op.state = Give
		return nil

	}

	return fmt.Errorf("can't initiate move of range in state %v", r.State())
}

func (op *MoveOp) Run() {
	s := op.state
	var err error

	// TODO: Keep track of the *previous* state so we can include it in error messages.

	for {
		switch s {
		case Failed:
			if err == nil {
				panic("move operation in Failed state with no error")
			}
			if op.Done != nil {
				op.Done(err)
			}

			log.Printf("move failed: range=%v, node=%v, err: %v", op.Range, op.Node, err)
			return

		case Complete:
			if op.Done != nil {
				op.Done(nil)
			}

			log.Printf("move complete: range=%v, node=%v", op.Range, op.Node)
			return

		case Init:
			panic("move operation re-entered init state")

		case Take:
			s, err = op.take()

		case Give:
			s, err = op.give()

		case Untake:
			s, err = op.untake()

		case FetchWait:
			s, err = op.fetchWait()

		case Serve:
			s, err = op.serve()

		case Drop:
			s, err = op.drop()
		}

		log.Printf("Move: %v -> %v", op.state, s)
		op.state = s
	}
}

func (op *MoveOp) take() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("take failed: %v", err)
	}

	p := r.Placement()
	if p == nil {
		return Failed, fmt.Errorf("take failed: Placement returned nil")
	}

	err = utils.Take(op.Roster, p)
	if err != nil {
		r.MustState(ranje.Ready) // ???
		return Failed, fmt.Errorf("take failed: %v", err)
	}

	return Give, nil
}

func (op *MoveOp) give() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("give failed: %v", err)
	}

	p, err := ranje.NewPlacement(r, op.Node)
	if err != nil {
		return Failed, fmt.Errorf("give failed: %v", err)
	}

	err = utils.Give(op.Roster, r, p)
	if err != nil {
		log.Printf("give failed: %v", err)

		// Clean up p. No return value.
		r.ClearNextPlacement()

		switch r.State() {
		case ranje.Placing:
			// During initial placement, we can just fail without cleanup. The
			// range is still not assigned. The balancer should retry the
			// placement, perhaps on a different node.
			r.MustState(ranje.PlaceError)
			return Failed, fmt.Errorf("give failed: %v", err)

		case ranje.Moving:
			// When moving, we have already taken the range from the src node,
			// but failed to give it to the dest! We must untake it from the
			// src, to avoid failing in a state where nobody has the range.
			return Untake, nil

		default:
			panic(fmt.Sprintf("impossible range state: %s", r.State()))
		}
	}

	// If the placement went straight to Ready, we're done. (This can happen
	// when the range isn't being moved from anywhere, or if the transfer
	// happens very quickly.)
	if p.State() == ranje.SpReady {
		return complete(r)
	}

	return FetchWait, nil
}

func (op *MoveOp) untake() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("untake failed: %v", err)
	}

	p := r.Placement()
	if p == nil {
		return Failed, fmt.Errorf("untake failed: Placement returned nil")
	}

	err = utils.Untake(op.Roster, p)
	if err != nil {
		// TODO: Try again?!
		return Failed, fmt.Errorf("untake failed: %v", err)
	}

	// The range is now ready again, because the current placement is ready.
	// (and the next placement is gone.)
	r.MustState(ranje.Ready)

	// Always transition into failed, because even though this step succeeded
	// and service has been restored to src, the move was a failure.
	// TODO: Return some info about *why* the move failed!
	return Failed, fmt.Errorf("move failed, but was rewound")
}

func (op *MoveOp) fetchWait() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("fetchWait failed: %v", err)
	}

	p := r.NextPlacement()
	if p == nil {
		return Failed, fmt.Errorf("fetchWait failed: NextPlacement is nil")
	}

	err = p.FetchWait()
	if err != nil {
		return Failed, fmt.Errorf("fetchWait failed: %v", err)
	}

	return Serve, nil
}

func (op *MoveOp) serve() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("serve failed: %v", err)
	}

	p := r.NextPlacement()
	if p == nil {
		return Failed, fmt.Errorf("serve failed: NextPlacement is nil")
	}

	err = utils.Serve(op.Roster, p)
	if err != nil {
		return Failed, fmt.Errorf("serve failed: %v", err)
	}

	switch r.State() {
	case ranje.Moving:
		// This is a range move, so even though the next placement is ready to
		// serve, we still have to clean up the current placement. We could mark
		// the range as ready now, to minimize the not-ready window, but a drop
		// operation should be fast, and it would be weird.
		return Drop, nil

	case ranje.Placing:
		// This is an initial placement, so we have no previous node to drop
		// data from. We're done.
		return complete(r)

	default:
		panic(fmt.Sprintf("impossible range state: %s", r.State()))
	}
}

func (op *MoveOp) drop() (state, error) {
	r, err := op.Keyspace.GetByIdent(op.Range)
	if err != nil {
		return Failed, fmt.Errorf("drop failed: %v", err)
	}

	p := r.Placement()
	if p == nil {
		return Failed, fmt.Errorf("drop failed: Placement is nil")
	}

	err = utils.Drop(op.Roster, p)
	if err != nil {
		return Failed, fmt.Errorf("drop failed: %v", err)
	}

	return complete(r)
}

func complete(r *ranje.Range) (state, error) {
	r.CompleteNextPlacement()
	r.MustState(ranje.Ready)
	return Complete, nil
}
