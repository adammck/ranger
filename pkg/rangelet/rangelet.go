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
	sync.RWMutex // guards info (keys *and* values) and callbacks

	n api.Node
	s api.Storage

	// TODO: Store abstract "node load" or "node status"? Drain is just a desire
	//       for all of the ranges to be moved away. Overload is just a desire
	//       for *some* ranges to be moved.
	xWantDrain uint32

	gracePeriod time.Duration

	srv *NodeServer

	// Holds functions to be called when a specific range leaves a state. This
	// is just for testing. Register callbacks via the OnLeaveState method.
	callbacks map[callback]func()
}

type callback struct {
	rID   ranje.Ident
	state state.RemoteState
}

func NewRangelet(n api.Node, sr grpc.ServiceRegistrar, s api.Storage) *Rangelet {
	r := &Rangelet{
		info: map[ranje.Ident]*info.RangeInfo{},
		n:    n,
		s:    s,

		gracePeriod: 1 * time.Second,
		callbacks:   map[callback]func(){},
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

func (r *Rangelet) runThenUpdateState(rID ranje.Ident, old state.RemoteState, success state.RemoteState, failure state.RemoteState, f func() error) {
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
	// delete them. This happens when a PrepareAddShard fails, or a DropShard
	// succeeds; either way, the range is gone.
	if s == state.NsNotFound {
		delete(r.info, rID)
		r.runCallback(rID, old, s)
		log.Printf("range deleted: %v", rID)
		return
	}

	ri, ok := r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished in runThenUpdateState! (rID=%v, s=%s)", rID, s))
	}

	ri.State = s
	r.runCallback(rID, old, s)
	log.Printf("state is now %v (rID=%v)", s, rID)
}

func (r *Rangelet) give(rm ranje.Meta, parents []api.Parent) (info.RangeInfo, error) {
	rID := rm.Ident
	r.Lock()

	ri, ok := r.info[rID]
	if ok {
		defer r.Unlock()

		if ri.State == state.NsLoading || ri.State == state.NsInactive {
			log.Printf("got redundant Give")
			return *ri, nil
		}

		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Give: %v", ri.State)
	}

	// Range is not currently known, so can be added.

	ri = &info.RangeInfo{
		Meta:  rm,
		State: state.NsLoading,
	}
	r.info[rID] = ri
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsLoading, state.NsInactive, state.NsNotFound, func() error {
			return r.n.PrepareAddRange(rm, parents)
		})
	})

	// PrepareAddRange either completed, or is still running but we don't want
	// to wait any longer. Fetch infos again to find out.
	r.Lock()
	defer r.Unlock()
	ri, ok = r.info[rID]

	if !ok {
		// The range has vanished, because runThenUpdateState saw that it was
		// NotFound and special-cased it. Return something tht kind of looks
		// right, so the controller knows what to do.
		//
		// TODO: Remove this (and return NotFound directly), so the controller
		//       sees the same version of the state whether updated by probe or
		//       by response from RPC.
		return info.RangeInfo{
			Meta:  ranje.Meta{Ident: rID},
			State: state.NsNotFound,
		}, nil
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

	if ri.State == state.NsActivating || ri.State == state.NsActive {
		log.Printf("got redundant Serve")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != state.NsInactive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", ri.State)
	}

	// State is NsInactive

	ri.State = state.NsActivating
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsActivating, state.NsActive, state.NsInactive, func() error {
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

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return info.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	if ri.State == state.NsDeactivating || ri.State == state.NsInactive {
		log.Printf("got redundant Take")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != state.NsActive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", ri.State)
	}

	// State is NsActive

	ri.State = state.NsDeactivating
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsDeactivating, state.NsInactive, state.NsActive, func() error {
			return r.n.PrepareDropRange(rID)
		})
	})

	r.Lock()
	defer r.Unlock()

	ri, ok = r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished from infos during take! (rID=%v)", rID))
	}

	return *ri, nil
}

func (r *Rangelet) drop(rID ranje.Ident) (info.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		log.Printf("got redundant Drop (no such range; maybe drop complete)")

		// Return success! The caller wanted the range to be dropped, and we
		// don't have the range. So (hopefully) we dropped it already.
		return notFound(rID), nil
	}

	if ri.State == state.NsDropping {
		log.Printf("got redundant Drop (drop in progress)")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != state.NsInactive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	// State is NsInactive

	ri.State = state.NsDropping
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, state.NsDropping, state.NsNotFound, state.NsInactive, func() error {
			return r.n.DropRange(rID)
		})
	})

	r.Lock()
	defer r.Unlock()

	ri, ok = r.info[rID]
	if !ok {
		// As above, the range being gone is success.
		return notFound(rID), nil
	}

	// The range is still here.
	// DropRange is presumably still running.
	return *ri, nil
}

// TODO: This method should be able to return multiple ranges. Might be called
//       during an operation where both src and dest are on this node. Currently
//       just returns the first one it finds.
func (r *Rangelet) Find(k ranje.Key) (ranje.Ident, bool) {
	for _, ri := range r.info {

		// Play dumb in some cases: a range can be known to the rangelet but
		// unknown to the client, in these states. (Find might have been called
		// before PrepareAddShard has returned, or while DropShard is still in
		// progress.) The client should check the state anyway, but this makes
		// the contract simpler.
		if ri.State == state.NsLoading || ri.State == state.NsDropping {
			continue
		}

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
	rostLI.Splits = make([]ranje.Key, len(rgltLI.Splits))
	for i, s := range rgltLI.Splits {
		rostLI.Splits[i] = s
	}
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

// State returns the state that the given range is currently in, or NsNotFound
// if the range doesn't exist. This should not be used for anything other than
// sanity-checking and testing. Clients should react to changes via the Node
// interface.
func (rglt *Rangelet) State(rID ranje.Ident) state.RemoteState {
	rglt.Lock()
	defer rglt.Unlock()

	r, ok := rglt.info[rID]
	if !ok {
		return state.NsNotFound
	}

	return r.State
}

// OnLeaveState registers a callback function which will be called when the
// given range transitions out of the given state. This is just for testing.
// TODO: Maybe just make this OnChangeState so FakeNode can do what it likes?
func (rglt *Rangelet) OnLeaveState(rID ranje.Ident, s state.RemoteState, f func()) {
	rglt.Lock()
	defer rglt.Unlock()
	rglt.callbacks[callback{rID: rID, state: s}] = f
}

// Caller must hold lock.
func (rglt *Rangelet) runCallback(rID ranje.Ident, old, new state.RemoteState) {
	cb := callback{rID: rID, state: old}

	f, ok := rglt.callbacks[cb]
	if !ok {
		return
	}

	delete(rglt.callbacks, cb)
	f()
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

func notFound(rID ranje.Ident) info.RangeInfo {
	return info.RangeInfo{
		Meta:  ranje.Meta{Ident: rID},
		State: state.NsNotFound,
		Info:  info.LoadInfo{},
	}
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
