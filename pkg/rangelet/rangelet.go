package rangelet

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Rangelet struct {
	info map[ranje.Ident]*info.RangeInfo
	sync.RWMutex

	xWantDrain uint32

	srv *NodeServer
}

func NewRangelet() *Rangelet {
	r := &Rangelet{
		info: map[ranje.Ident]*info.RangeInfo{},
	}

	// TODO: Serve this when?
	r.srv = NewNodeServer(r)

	return r
}

var ErrNotFound = errors.New("not found")

func (r *Rangelet) give(rm ranje.Meta) (*info.RangeInfo, error) {
	rID := rm.Ident

	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		ri = &info.RangeInfo{
			Meta:  rm,
			State: state.NsPreparing,
		}
		r.info[rID] = ri
		return ri, nil
	}

	switch ri.State {
	case state.NsPreparing, state.NsPreparingError, state.NsPrepared:
		// We already know about this range, and it's in one of the states that
		// indicate a previous Give. This is just a duplicate. Don't change any
		// state, just return the RangeInfo to let the controller know whether
		// we need more time or it can advance to Ready.
		log.Printf("got redundant Give")

	default:
		return ri, status.Errorf(codes.InvalidArgument, "invalid state for redundant Give: %v", ri.State)
	}

	return ri, nil
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
		// Actual state transition.
		ri.State = state.NsReadying

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
		// Actual state transition.
		ri.State = state.NsTaking

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
		// Actual state transition. We don't actually drop anything here, only
		// claim that we are doing so, to simulate a slow client. Test must call
		// FinishDrop to move to NsDropped.
		ri.State = state.NsDropping

	case state.NsDropping:
		log.Printf("got redundant Drop (drop in progress)")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	return ri, nil
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
