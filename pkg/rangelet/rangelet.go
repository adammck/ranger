package rangelet

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Rangelet struct {
	info         map[ranje.Ident]*info.RangeInfo
	sync.RWMutex // guards info (keys *and* values)

	n api.Node
	s api.Storage

	// TODO: Store abstract "node load" or "node status"? Drain is just a desire
	//       for all of the ranges to be moved away. Overload is just a desire
	//       for *some* ranges to be moved.
	xWantDrain uint32

	gracePeriod time.Duration

	srv *NodeServer
}

func NewRangelet(n api.Node, sr grpc.ServiceRegistrar, s api.Storage) *Rangelet {
	r := &Rangelet{
		info: map[ranje.Ident]*info.RangeInfo{},
		n:    n,
		s:    s,

		gracePeriod: 1 * time.Second,
	}

	for _, ri := range s.Read() {
		r.info[ri.Meta.Ident] = ri
	}

	// Can't think of any reason this would be useful outside of a test.
	if sr != nil {
		r.srv = NewNodeServer(r)
		r.srv.Register(sr)
	}

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

func (r *Rangelet) give(rm ranje.Meta, parents []api.Parent) (info.RangeInfo, error) {
	rID := rm.Ident
	r.Lock()

	ri, ok := r.info[rID]
	if ok {
		defer r.Unlock()

		if ri.State == state.NsPreparing || ri.State == state.NsPrepared {
			log.Printf("got redundant Give")
			return *ri, nil
		}

		// TODO: Allow retry if in PreparingError
		if ri.State == state.NsPreparingError {
			log.Printf("got Give in NsPreparingError")
			return *ri, nil
		}

		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Give: %v", ri.State)
	}

	// Range is not currently known, so can be added.

	ri = &info.RangeInfo{
		Meta:  rm,
		State: state.NsPreparing,
	}
	r.info[rID] = ri
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsPrepared, state.NsPreparingError, func() error {
			return r.n.PrepareAddRange(rm, parents)
		})
	})

	// PrepareAddRange either completed, or is still running but we don't want
	// to wait any longer. Fetch infos again to find out.
	r.Lock()
	defer r.Unlock()
	ri, ok = r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished during Give! (rID=%v)", rID))
	}

	return *ri, nil
}

func (r *Rangelet) serve(rID ranje.Ident) (info.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return info.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	if ri.State == state.NsReadying || ri.State == state.NsReady {
		log.Printf("got redundant Serve")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != state.NsPrepared {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", ri.State)
	}

	// State is NsPrepared

	ri.State = state.NsReadying
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsReady, state.NsReadyingError, func() error {
			return r.n.AddRange(rID)
		})
	})

	r.Lock()
	defer r.Unlock()

	// Fetch the current rangeinfo again!
	ri, ok = r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished from infos during serve! (rID=%v)", rID))
	}

	return *ri, nil
}

func (r *Rangelet) take(rID ranje.Ident) (info.RangeInfo, error) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		return info.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
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
		return info.RangeInfo{}, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", ri.State)
	}

	return *ri, nil
}

func (r *Rangelet) drop(rID ranje.Ident) (info.RangeInfo, error) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		log.Printf("got redundant Drop (no such range; maybe drop complete)")
		return info.RangeInfo{}, ErrNotFound
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
		return info.RangeInfo{}, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	return *ri, nil
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

func (r *Rangelet) gatherLoadInfo() error {

	// Can't gather the range IDs in advance and release the lock, because the
	// GetLoadInfo implementations should be able to expect that rangelet will
	// never call them with a range which doesn't exist.
	r.Lock()
	defer r.Unlock()

	for rID, ri := range r.info {
		info, err := r.n.GetLoadInfo(rID)

		if err == api.NotFound {
			// No problem. The Rangelet knows about this range, but the client
			// doesn't, for whatever reason. Probably racing PrepareAddRange.
			log.Printf("GetLoadInfo(%v): NotFound", rID)
			continue

		} else if err != nil {
			log.Printf("GetLoadInfo(%v): %v", rID, err)
			continue
		}

		//log.Printf("GetLoadInfo(%v): %#v", rID, info)
		updateLoadInfo(&ri.Info, info)
	}

	return nil
}

// updateLoadInfo updates the given roster/info.LoadInfo (which is what we
// store in Rangelet.info, via roster/info.RangeInfo) with the info from the
// given rangelet.LoadInfo. This is very nasty and hopefully temporary.
// TODO: Remove once roster/info import is gone from rangelet.
func updateLoadInfo(rostLI *info.LoadInfo, rgltLI api.LoadInfo) {
	rostLI.Keys = uint64(rgltLI.Keys)
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

func (r *Rangelet) rangeInfo(rID ranje.Ident) (info.RangeInfo, bool) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if ok {
		return *ri, true
	}

	return info.RangeInfo{}, false
}

func withTimeout(timeout time.Duration, f func()) bool {
	ch := make(chan struct{})
	go func() {
		f()
		close(ch)
	}()

	select {
	case <-ch:
		log.Print("finished!")
		return true

	case <-time.After(timeout):
		log.Print("timed out")
		return false
	}
}

// Just for tests.
// TODO: Remove this somehow? Only TestNodes needs it.
func (r *Rangelet) SetGracePeriod(d time.Duration) {
	r.gracePeriod = d
}
