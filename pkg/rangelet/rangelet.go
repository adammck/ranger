package rangelet

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Rangelet struct {
	info map[ranje.Ident]*info.RangeInfo
	sync.RWMutex

	n Node
	s Storage

	xWantDrain uint32

	srv *NodeServer
}

func NewRangelet(n Node, sr grpc.ServiceRegistrar, s Storage) *Rangelet {
	r := &Rangelet{
		info: map[ranje.Ident]*info.RangeInfo{},
		n:    n,
		s:    s,
	}

	for _, ri := range s.Read() {
		r.info[ri.Meta.Ident] = ri
	}

	r.srv = NewNodeServer(r)
	r.srv.Register(sr)

	return r
}

var ErrNotFound = errors.New("not found")

func (r *Rangelet) runThenUpdateState(rID ranje.Ident, success state.RemoteState, failure state.RemoteState, f func() error) {
	err := f()

	var s state.RemoteState
	if err != nil {
		// TODO: Pass this error (from the client) back to the controller.
		s = failure

	} else {
		s = success
	}

	r.Lock()
	defer r.Unlock()

	// Special case: Ranges are never actually in NotFound; it's a signal to
	// delete them.
	if s == state.NsNotFound {
		delete(r.info, rID)
		log.Printf("range deleted: %v", rID)
		return
	}

	ri, ok := r.info[rID]
	if !ok {
		// The range has vanished from the map??
		panic("this should not happen!")
	}

	log.Printf("state is now %v (rID=%v)", s, rID)
	ri.State = s
}

func (r *Rangelet) give(rm ranje.Meta, parents []Parent) (info.RangeInfo, error) {
	rID := rm.Ident

	// TODO: Release the lock while calling PrepareAddRange.
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		ri = &info.RangeInfo{
			Meta:  rm,
			State: state.NsPreparing,
		}
		r.info[rID] = ri

		// Copy the info, because it might change sooner than we can return!
		tmp := *ri

		// TODO: Wait for a brief period before returning, so fast clients don't
		//       have to wait for the next Give to tell the controller they have
		//       finished preparing.
		go r.runThenUpdateState(rID, state.NsPrepared, state.NsPreparingError, func() error {
			return r.n.PrepareAddRange(rm, parents)
		})

		return tmp, nil
	}

	switch ri.State {
	case state.NsPreparing, state.NsPreparingError, state.NsPrepared:
		// We already know about this range, and it's in one of the states that
		// indicate a previous Give. This is just a duplicate. Don't change any
		// state, just return the RangeInfo to let the controller know whether
		// we need more time or it can advance to Ready.
		log.Printf("got redundant Give")

	default:
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for redundant Give: %v", ri.State)
	}

	return *ri, nil
}

func (r *Rangelet) serve(rID ranje.Ident) (*info.RangeInfo, error) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		return ri, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	switch ri.State {
	case state.NsPrepared:
		ri.State = state.NsReadying
		go r.runThenUpdateState(rID, state.NsReady, state.NsReadyingError, func() error {
			return r.n.AddRange(rID)
		})

	case state.NsReadying, state.NsReady:
		log.Printf("got redundant Serve")

	default:
		return ri, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", ri.State)
	}

	return ri, nil
}

func (r *Rangelet) take(rID ranje.Ident) (*info.RangeInfo, error) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	switch ri.State {
	case state.NsReady:
		ri.State = state.NsTaking
		go r.runThenUpdateState(rID, state.NsTaken, state.NsTakingError, func() error {
			return r.n.PrepareDropRange(rID)
		})

	case state.NsTaking, state.NsTaken:
		log.Printf("got redundant Take")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", ri.State)
	}

	return ri, nil
}

func (r *Rangelet) drop(rID ranje.Ident) (*info.RangeInfo, error) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		log.Printf("got redundant Drop (no such range; maybe drop complete)")
		return ri, ErrNotFound
	}

	switch ri.State {
	case state.NsTaken:
		ri.State = state.NsDropping
		go r.runThenUpdateState(rID, state.NsNotFound, state.NsDroppingError, func() error {
			return r.n.DropRange(rID)
		})

	case state.NsDropping:
		log.Printf("got redundant Drop (drop in progress)")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	return ri, nil
}

func (r *Rangelet) Find(k ranje.Key) (ranje.Ident, bool) {
	for _, ri := range r.info {
		if ri.Meta.Contains(k) {
			return ri.Meta.Ident, true
		}
	}

	return ranje.ZeroRange, false
}

func (r *Rangelet) Len() int {
	r.RLock()
	defer r.RUnlock()
	return len(r.info)
}

func (r *Rangelet) walk(f func(*info.RangeInfo)) {
	r.RLock()
	defer r.RUnlock()

	for _, ri := range r.info {
		f(ri)
	}
}

func (r *Rangelet) wantDrain() bool {
	b := atomic.LoadUint32(&r.xWantDrain)
	return b == 1
}

func (r *Rangelet) SetWantDrain(b bool) {
	var v uint32
	if b {
		v = 1
	}

	atomic.StoreUint32(&r.xWantDrain, v)
}

// State returns the state that the given range is currently in. This should not
// be used for anything other than sanity-checking. Clients should react to
// changes via the Node interface.
func (r *Rangelet) State(rID ranje.Ident) state.RemoteState {
	return r.info[rID].State
}
