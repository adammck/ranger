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

type watcher struct {
	changed func(*api.RangeInfo) bool
	removed func()
}

type Rangelet struct {
	info         map[api.RangeID]*api.RangeInfo
	watchers     []*watcher
	sync.RWMutex // guards info (keys *and* values), watchers, and callbacks

	n api.Node
	s api.Storage

	// TODO: Store abstract "node load" or "node status"? Drain is just a desire
	// for all of the ranges to be moved away. Overload is just a desire for
	// *some* ranges to be moved.
	xWantDrain uint32

	gracePeriod time.Duration

	// Holds functions to be called when a specific range leaves a state. This
	// is just for testing. Register callbacks via the OnLeaveState method.
	callbacks map[callback]func()
}

type callback struct {
	rID   api.RangeID
	state api.RemoteState
}

func New(n api.Node, sr grpc.ServiceRegistrar, s api.Storage) *Rangelet {
	r := newRangelet(n, s)

	// Can't think of any reason this would be useful outside of a test.
	if sr != nil {
		server := newNodeServer(r)
		server.Register(sr)
	}

	return r
}

// newRangelet constructs a new Rangelet without a NodeServer. This is only
// really useful during testing. I cannot think of any reason a client would
// want a rangelet with no gRPC interface to receive range assignments through.
func newRangelet(n api.Node, s api.Storage) *Rangelet {
	r := &Rangelet{
		info:     map[api.RangeID]*api.RangeInfo{},
		watchers: []*watcher{},

		n: n,
		s: s,

		gracePeriod: 1 * time.Second,
		callbacks:   map[callback]func(){},
	}

	for _, ri := range s.Read() {
		// Can't be any watchers yet.
		r.info[ri.Meta.Ident] = ri
	}

	return r
}

var ErrNotFound = errors.New("not found")

func (r *Rangelet) runThenUpdateState(rID api.RangeID, old api.RemoteState, success api.RemoteState, failure api.RemoteState, f func() error) {
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

	ri, ok := r.info[rID]
	if !ok {
		panic(fmt.Sprintf("range vanished in runThenUpdateState! (rID=%v, s=%s)", rID, s))
	}

	ri.State = s

	// Special case: Ranges are never actually in NotFound; it's a signal to
	// delete them. This happens when a Prepare fails, or a Drop succeeds;
	// either way, the range is gone.
	if ri.State == api.NsNotFound {
		delete(r.info, rID)
	}

	r.runCallback(rID, old, s)
	log.Printf("R%s: %s -> %s", rID, old, s)
	r.notifyWatchers(ri)
}

func (r *Rangelet) prepare(rm api.Meta, parents []api.Parent) (api.RangeInfo, error) {
	rID := rm.Ident
	r.Lock()

	ri, ok := r.info[rID]
	if ok {
		defer r.Unlock()

		if ri.State == api.NsPreparing || ri.State == api.NsInactive {
			return *ri, nil
		}

		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Prepare: %v", ri.State)
	}

	// Range is not currently known, so can be added.

	log.Printf("R%s: nil -> %s", rID, api.NsPreparing)
	ri = &api.RangeInfo{
		Meta:  rm,
		State: api.NsPreparing,
	}
	r.info[rID] = ri
	r.notifyWatchers(ri)
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsPreparing, api.NsInactive, api.NsNotFound, func() error {
			return r.n.Prepare(rm, parents)
		})
	})

	// Prepare either completed, or is still running but we don't want to wait
	// any longer. Fetch infos again to find out.
	r.Lock()
	defer r.Unlock()
	ri, ok = r.info[rID]

	if !ok {
		// The range has vanished, because runThenUpdateState saw that it was
		// NotFound and special-cased it. Return something tht kind of looks
		// right, so the controller knows what to do.
		//
		// TODO: Remove this (and return NotFound directly), so the controller
		// sees the same version of the state whether updated by probe or by
		// response from RPC.
		return api.RangeInfo{
			Meta:  api.Meta{Ident: rID},
			State: api.NsNotFound,
		}, nil
	}

	return *ri, nil
}

func (r *Rangelet) serve(rID api.RangeID) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return api.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Activate unknown range: %v", rID)
	}

	if ri.State == api.NsActivating || ri.State == api.NsActive {
		r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsInactive {
		r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Activate: %v", ri.State)
	}

	// State is NsInactive

	log.Printf("R%s: %s -> %s", rID, ri.State, api.NsActivating)
	ri.State = api.NsActivating
	r.notifyWatchers(ri)
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsActivating, api.NsActive, api.NsInactive, func() error {
			return r.n.Activate(rID)
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

func (r *Rangelet) take(rID api.RangeID) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()
		return api.RangeInfo{}, status.Errorf(codes.InvalidArgument, "can't Deactivate unknown range: %v", rID)
	}

	if ri.State == api.NsDeactivating || ri.State == api.NsInactive {
		r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsActive {
		r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Deactivate: %v", ri.State)
	}

	// State is NsActive

	log.Printf("R%s: %s -> %s", rID, ri.State, api.NsDeactivating)
	ri.State = api.NsDeactivating
	r.notifyWatchers(ri)
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsDeactivating, api.NsInactive, api.NsActive, func() error {
			return r.n.Deactivate(rID)
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

func (r *Rangelet) drop(rID api.RangeID) (api.RangeInfo, error) {
	r.Lock()

	ri, ok := r.info[rID]
	if !ok {
		r.Unlock()

		// Return success! The caller wanted the range to be dropped, and we
		// don't have the range. So (hopefully) we dropped it already.
		return notFound(rID), nil
	}

	if ri.State == api.NsDropping {
		defer r.Unlock()
		return *ri, nil
	}

	if ri.State != api.NsInactive {
		defer r.Unlock()
		return *ri, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", ri.State)
	}

	// State is NsInactive

	log.Printf("R%s: %s -> %s", rID, ri.State, api.NsDropping)
	ri.State = api.NsDropping
	r.notifyWatchers(ri)
	r.Unlock()

	withTimeout(r.gracePeriod, func() {
		r.runThenUpdateState(rID, api.NsDropping, api.NsNotFound, api.NsInactive, func() error {
			return r.n.Drop(rID)
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
	// Drop is presumably still running.
	return *ri, nil
}

// TODO: This method should be able to return multiple ranges. Might be called
// during an operation where both src and dest are on this node. Currently just
// returns the first one it finds.
func (r *Rangelet) Find(k api.Key) (api.RangeID, bool) {
	for _, ri := range r.info {

		// Play dumb in some cases: a range can be known to the rangelet but
		// unknown to the client, in these states. (Find might have been called
		// before Prepare has returned, or while Drop is still in progress.) The
		// client should check the state anyway, but this makes the contract
		// simpler.
		if ri.State == api.NsPreparing || ri.State == api.NsDropping {
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
func (r *Rangelet) ForceDrop(rID api.RangeID) error {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if !ok {
		return fmt.Errorf("no such range: %d", rID)
	}

	delete(r.info, rID)

	// Only need to update state for log and watchers.
	log.Printf("R%s: %s -> %s", rID, ri.State, api.NsNotFound)
	ri.State = api.NsNotFound
	r.notifyWatchers(ri)

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
			// doesn't, for whatever reason. Probably racing Prepare.
			continue

		} else if err != nil {
			// Also no problem?
			// TODO: This seems like it should be a problem??
			continue
		}

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

func (r *Rangelet) walk(f func(*api.RangeInfo) bool) {
	r.RLock()
	defer r.RUnlock()

	for _, ri := range r.info {
		ok := f(ri)

		// stop walking if func returns false.
		if !ok {
			break
		}
	}
}

func (r *Rangelet) watch(f func(*api.RangeInfo) bool) {
	r.Lock()

	// First call func for each of the ranges and their current state. We're
	// inside the rangelet lock, so these will not change under us.
	for _, ri := range r.info {
		ok := f(ri)

		// If the callback returns false (i.e. something went wrong, don't call
		// me again), unlock and bail out without even creating the watcher.
		if !ok {
			r.Unlock()
			return
		}
	}

	// Register a watcher, which will be called any time a range changes state.
	// We're still under the rangelet lock, so no ranges will have changed state
	// during this method. That's important, because missing a transition would
	// cause a client to be out of sync indefinitely.

	wg := sync.WaitGroup{}
	wg.Add(1)

	r.watchers = append(r.watchers,
		&watcher{
			changed: f,
			removed: wg.Done,
		})

	// Now that watcher is registered, we can unlock, allowing any pending and
	// future changes to be applied.
	r.Unlock()

	// Block until the watcher is removed, which will only happen when the func
	// returns false.
	wg.Wait()
}

// Caller must hold rangelet write lock.
func (r *Rangelet) notifyWatchers(ri *api.RangeInfo) {
	for i, w := range r.watchers {
		ok := w.changed(ri)
		if !ok {
			r.removeWatcher(i)
			w.removed()
		}
	}
}

// Caller must hold rangelet write lock.
func (r *Rangelet) removeWatcher(i int) {
	// lol, golang
	// https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order
	r.watchers[i] = r.watchers[len(r.watchers)-1]
	r.watchers[len(r.watchers)-1] = nil
	r.watchers = r.watchers[:len(r.watchers)-1]
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
func (rglt *Rangelet) State(rID api.RangeID) api.RemoteState {
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
func (rglt *Rangelet) OnLeaveState(rID api.RangeID, s api.RemoteState, f func()) {
	rglt.Lock()
	defer rglt.Unlock()
	rglt.callbacks[callback{rID: rID, state: s}] = f
}

// Caller must hold lock.
func (rglt *Rangelet) runCallback(rID api.RangeID, old, new api.RemoteState) {
	cb := callback{rID: rID, state: old}

	f, ok := rglt.callbacks[cb]
	if !ok {
		return
	}

	delete(rglt.callbacks, cb)
	f()
}

func (r *Rangelet) rangeInfo(rID api.RangeID) (api.RangeInfo, bool) {
	r.Lock()
	defer r.Unlock()

	ri, ok := r.info[rID]
	if ok {
		return *ri, true
	}

	return api.RangeInfo{}, false
}

func notFound(rID api.RangeID) api.RangeInfo {
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
