package rangelet

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Rangelet struct {
	info         map[api.Ident]*api.RangeInfo
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
	rID   api.Ident
	state api.RemoteState
}

func NewRangelet(n api.Node, sr grpc.ServiceRegistrar, s api.Storage) *Rangelet {
	r := &Rangelet{
		info: map[api.Ident]*api.RangeInfo{},
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

func (r *Rangelet) runThenUpdateState(rID api.Ident, old api.RemoteState, success api.RemoteState, failure api.RemoteState, f func() error) {
	err := f()

	var s api.RemoteState
	if err != nil {
		// TODO: Pass this error (from the client) back to the controller.
		s = failure

	} else {
		s = success
	}

	r.Lock()
	defer r.Unlock()

	// Special case: Ranges are never actually in NotFound; it's a signal to
	// delete them. This happens when a PrepareAddRange fails, or a DropRange
	// succeeds; either way, the range is gone.
	if s == api.NsNotFound {
		delete(r.info, rID)
		r.runCallback(rID, old, s)
		log.Printf("[rglt] range deleted: %v", rID)
		return
	}

	ri, ok := r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished in runThenUpdateState! (rID=%v, s=%s)", rID, s))
	}

	ri.State = s
	r.runCallback(rID, old, s)
	log.Printf("[rglt] state is now %v (rID=%v)", s, rID)
}

func (r *Rangelet) give(rm api.Meta, parents []api.Parent) (api.RangeInfo, error) {
	rID := rm.Ident
	r.Lock()

	ri, ok := r.info[rID]
	if ok {
		defer r.Unlock()

		if ri.State == api.NsLoading || ri.State == api.NsInactive {
			log.Printf("[rglt] got redundant Give")
			return *ri, nil
		}

		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Give: %v", ri.State)
	}

	// Range is not currently known, so can be added.

	ri = &api.RangeInfo{
		Meta:  rm,
		State: api.NsLoading,
	}
	r.info[rID] = ri
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsLoading, api.NsInactive, api.NsNotFound, func() error {
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
		return api.RangeInfo{
			Meta:  api.Meta{Ident: rID},
			State: api.NsNotFound,
		}, nil
	}

	return *ri, nil
}

func (r *Rangelet) serve(rID api.Ident) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return api.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	if ri.State == api.NsActivating || ri.State == api.NsActive {
		log.Printf("[rglt] got redundant Serve")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsInactive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", ri.State)
	}

	// State is NsInactive

	ri.State = api.NsActivating
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsActivating, api.NsActive, api.NsInactive, func() error {
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

func (r *Rangelet) take(rID api.Ident) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return api.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	if ri.State == api.NsDeactivating || ri.State == api.NsInactive {
		log.Printf("[rglt] got redundant Take")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsActive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", ri.State)
	}

	// State is NsActive

	ri.State = api.NsDeactivating
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsDeactivating, api.NsInactive, api.NsActive, func() error {
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

func (r *Rangelet) drop(rID api.Ident) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		log.Printf("[rglt] got redundant Drop (no such range; maybe drop complete)")

		// Return success! The caller wanted the range to be dropped, and we
		// don't have the range. So (hopefully) we dropped it already.
		return notFound(rID), nil
	}

	if ri.State == api.NsDropping {
		log.Printf("[rglt] got redundant Drop (drop in progress)")
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsInactive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	// State is NsInactive

	ri.State = api.NsDropping
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsDropping, api.NsNotFound, api.NsInactive, func() error {
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
func (r *Rangelet) Find(k api.Key) (api.Ident, bool) {
	for _, ri := range r.info {

		// Play dumb in some cases: a range can be known to the rangelet but
		// unknown to the client, in these states. (Find might have been called
		// before PrepareAddRange has returned, or while DropRange is still in
		// progress.) The client should check the state anyway, but this makes
		// the contract simpler.
		if ri.State == api.NsLoading || ri.State == api.NsDropping {
			continue
		}

		if ri.Meta.Contains(k) {
			return ri.Meta.Ident, true
		}
	}

	return api.ZeroRange, false
}

func (r *Rangelet) Len() int {
	r.RLock()
	defer r.RUnlock()
	return len(r.info)
}

// Force the Rangelet to forget about the given range, without calling any of
// the interface methods or performing any cleanup. It'll just vanish from probe
// responses. This should only ever be called by the node when it has decided to
// drop the range and wants to tell the controller that.
func (r *Rangelet) ForceDrop(rID api.Ident) error {
	r.Lock()
	defer r.Unlock()

	_, ok := r.info[rID]
	if !ok {
		return fmt.Errorf("no such range: %d", rID)
	}

	delete(r.info, rID)
	return nil
}

func (r *Rangelet) gatherLoadInfo() error {

	// Can't gather the range IDs in advance and release the lock, because the
	// GetLoadInfo implementations should be able to expect that rangelet will
	// never call them with a range which doesn't exist.
	r.Lock()
	defer r.Unlock()

	for rID, ri := range r.info {
		info, err := r.n.GetLoadInfo(rID)

		if err == api.ErrNotFound {
			// No problem. The Rangelet knows about this range, but the client
			// doesn't, for whatever reason. Probably racing PrepareAddRange.
			log.Printf("[rglt] GetLoadInfo(%v): NotFound", rID)
			continue

		} else if err != nil {
			log.Printf("[rglt] GetLoadInfo(%v): %v", rID, err)
			continue
		}

		//log.Printf("[rglt] GetLoadInfo(%v): %#v", rID, info)
		updateLoadInfo(&ri.Info, info)
	}

	return nil
}

// updateLoadInfo updates the given roster/info.LoadInfo (which is what we
// store in Rangelet.info, via roster/api.RangeInfo) with the info from the
// given rangelet.LoadInfo. This is very nasty and hopefully temporary.
// TODO: Remove once roster/info import is gone from rangelet.
func updateLoadInfo(rostLI *api.LoadInfo, rgltLI api.LoadInfo) {
	rostLI.Keys = int(rgltLI.Keys)
	rostLI.Splits = make([]api.Key, len(rgltLI.Splits))
	copy(rostLI.Splits, rgltLI.Splits)
}

func (r *Rangelet) walk(f func(*api.RangeInfo)) {
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
func (rglt *Rangelet) State(rID api.Ident) api.RemoteState {
	rglt.Lock()
	defer rglt.Unlock()

	r, ok := rglt.info[rID]
	if !ok {
		return api.NsNotFound
	}

	return r.State
}

// OnLeaveState registers a callback function which will be called when the
// given range transitions out of the given state. This is just for testing.
// TODO: Maybe just make this OnChangeState so FakeNode can do what it likes?
func (rglt *Rangelet) OnLeaveState(rID api.Ident, s api.RemoteState, f func()) {
	rglt.Lock()
	defer rglt.Unlock()
	rglt.callbacks[callback{rID: rID, state: s}] = f
}

// Caller must hold lock.
func (rglt *Rangelet) runCallback(rID api.Ident, old, new api.RemoteState) {
	cb := callback{rID: rID, state: old}

	f, ok := rglt.callbacks[cb]
	if !ok {
		return
	}

	delete(rglt.callbacks, cb)
	f()
}

func (r *Rangelet) rangeInfo(rID api.Ident) (api.RangeInfo, bool) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if ok {
		return *ri, true
	}

	return api.RangeInfo{}, false
}

func notFound(rID api.Ident) api.RangeInfo {
	return api.RangeInfo{
		Meta:  api.Meta{Ident: rID},
		State: api.NsNotFound,
		Info:  api.LoadInfo{},
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
		return true

	case <-time.After(timeout):
		return false
	}
}

// Just for tests.
// TODO: Remove this somehow? Only TestNodes needs it.
func (r *Rangelet) SetGracePeriod(d time.Duration) {
	r.gracePeriod = d
}
