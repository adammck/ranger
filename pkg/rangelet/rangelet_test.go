package rangelet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// For assert.Eventually
const waitFor = 500 * time.Millisecond
const tick = 10 * time.Millisecond

// https://stackoverflow.com/a/32620397
var maxTime = time.Unix(1<<63-62135596801, 999999999)

func Setup() (clockwork.FakeClock, *MockNode, *Rangelet) {
	c := clockwork.NewFakeClock()

	n := &MockNode{
		wgPrepare:    &sync.WaitGroup{},
		wgActivate:   &sync.WaitGroup{},
		wgDeactivate: &sync.WaitGroup{},
		wgDrop:       &sync.WaitGroup{},
	}

	stor := fake_storage.NewFakeStorage(nil)
	rglt := newRangelet(c, n, stor)
	return c, n, rglt
}

func TestPrepareFast(t *testing.T) {
	_, _, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.prepare(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check range was created.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check idempotency.
	ri, err = rglt.prepare(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestPrepareSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// Prepare will block.
	n.wgPrepare.Add(1)

	for i := 0; i < 2; i++ {

		// The first call to rglt.prepare will block forever in withTimeout,
		// because time is frozen for this test. Wait for the main thread to
		// reach that, and then advance to let it return.
		//
		// Subsequent calls will not sleep, because the first call is still
		// waiting in Prepare (because n.wgPrepare), even after the timeout.
		//
		// TODO: What should probably happen is that *all* calls to prepare,
		// including those which are no-op because the placement is already in
		// NsPreparing, should block for the grace period and return as soon as
		// the placement becomes NsInactive. That would also remove this dumb
		// if statement.
		if i == 0 {
			blockThenAdvance(t, c)
		}

		ri, err := rglt.prepare(m, p)
		require.NoError(t, err)
		assert.Equal(t, ri.Meta, m)
		assert.Equal(t, api.NsPreparing, ri.State)
	}

	called := atomic.LoadUint32(&n.nPrepare)
	assert.Equal(t, uint32(1), called)

	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsPreparing, ri.State)

	// Unblock Prepare.
	n.wgPrepare.Done()

	// Wait until range exists.
	assert.Eventually(t, func() bool {
		_, ok := rglt.rangeInfo(m.Ident)
		return ok
	}, waitFor, tick)

	ri, ok = rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)

	for i := 0; i < 2; i++ {
		ri, err := rglt.prepare(m, p)
		require.NoError(t, err)
		assert.Equal(t, ri.Meta, m)
		assert.Equal(t, api.NsInactive, ri.State)
	}
}

func TestPrepareErrorFast(t *testing.T) {
	_, n, rglt := Setup()
	n.erPrepare = errors.New("error from Prepare")

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.prepare(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsNotFound, ri.State)

	// Check that no range was created.
	ri, ok := rglt.rangeInfo(m.Ident)
	assert.False(t, ok)
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestPrepareErrorSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// Prepare will block, then return an error.
	n.erPrepare = errors.New("error from Prepare")
	n.wgPrepare.Add(1)

	// Prepare the range. Even though the client will eventually return error
	// from Prepare, the outer call (give succeeds because it will exceed the
	// grace period and respond with NsPreparing.
	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.prepare(m, p)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsPreparing, ri.State)
	}

	called := atomic.LoadUint32(&n.nPrepare)
	assert.Equal(t, uint32(1), called)

	// Unblock Prepare.
	n.wgPrepare.Done()

	// Wait until range vanishes (because Prepare returned error).
	require.Eventually(t, func() bool {
		_, ok := rglt.rangeInfo(m.Ident)
		return !ok
	}, waitFor, tick)
}

func setupServe(infos map[api.RangeID]*api.RangeInfo, m api.Meta) {
	infos[m.Ident] = &api.RangeInfo{
		Meta:  m,
		State: api.NsInactive,
	}
}

func TestServeFast(t *testing.T) {
	_, _, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	ri, err := rglt.serve(m.Ident, maxTime)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)

	// Check state became NsActive.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActive, ri.State)

	// Check idempotency.
	ri, err = rglt.serve(m.Ident, maxTime)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)
}

func TestServeSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	// Activate will take a long time!
	n.wgActivate.Add(1)

	// This one will give up waiting and return early.
	blockThenAdvance(t, c)
	ri, err := rglt.serve(m.Ident, maxTime)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActivating, ri.State)

	// Check that state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActivating, ri.State)

	// Unblock Activate.
	n.wgActivate.Done()

	// Wait until state is returned to NsActive.
	assert.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsActive
	}, waitFor, tick)

	for i := 0; i < 2; i++ {
		ri, err = rglt.serve(m.Ident, maxTime)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsActive, ri.State)
	}
}

func TestServeUnknown(t *testing.T) {
	_, _, rglt := Setup()

	ri, err := rglt.serve(1, maxTime)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Activate unknown range: 1")
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestServeErrorFast(t *testing.T) {
	_, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	n.erActivate = errors.New("error from Activate")

	ri, err := rglt.serve(m.Ident, maxTime)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// State was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestServeErrorSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	// Activate will block, then return an error.
	n.erActivate = errors.New("error from Activate")
	n.wgActivate.Add(1)

	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.serve(m.Ident, maxTime)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsActivating, ri.State)
	}

	called := atomic.LoadUint32(&n.nActivate)
	assert.Equal(t, uint32(1), called)

	// Unblock Activate.
	n.wgActivate.Done()

	// Wait until state returns to Prepared (because Activate returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsInactive
	}, waitFor, tick)
}

func setupDeactivate(rglt *Rangelet, m api.Meta, expire time.Time) {
	rglt.info[m.Ident] = &api.RangeInfo{
		Meta:   m,
		State:  api.NsActive,
		Expire: expire,
	}
}

func TestDeactivateFast(t *testing.T) {
	c, _, rglt := Setup()

	m := api.Meta{Ident: 1}
	exp := c.Now().Add(10 * time.Minute)
	setupDeactivate(rglt, m, exp)

	ri, err := rglt.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check state became NsInactive.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check idempotency.
	ri, err = rglt.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestDeactivateSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	exp := c.Now().Add(10 * time.Minute)
	setupDeactivate(rglt, m, exp)

	n.wgDeactivate.Add(1)

	// Try to call serve a few times.
	// Should be the same response.
	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDeactivating, ri.State)
	}

	// Check that state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsDeactivating, ri.State)

	// Unblock Deactivate.
	n.wgDeactivate.Done()

	// Wait until state is updated.
	assert.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsInactive
	}, waitFor, tick)

	for i := 0; i < 2; i++ {
		ri, err := rglt.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsInactive, ri.State)
	}
}

func TestDeactivateExpire(t *testing.T) {
	c, n, rglt := Setup()

	ctx, canc := context.WithCancel(context.Background())
	rglt.startExpireRoutine(ctx)
	t.Cleanup(canc)

	m := api.Meta{Ident: 1}
	exp := c.Now().Add(10 * time.Minute)
	setupDeactivate(rglt, m, exp)

	// Advance time far enough that the lease has expired.
	c.Advance(11 * time.Minute)

	// Assert that deactivate was called, even though we didn't call it.
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&n.nDeactivate) > 0
	}, waitFor, tick)

	// Check that state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	require.Equal(t, api.NsInactive, ri.State)
}

func TestDeactivateUnknown(t *testing.T) {
	_, _, rglt := Setup()

	ri, err := rglt.take(1)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Deactivate unknown range: 1")
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestDeactivateErrorFast(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	exp := c.Now().Add(10 * time.Minute)
	setupDeactivate(rglt, m, exp)

	n.erDeactivate = errors.New("error from Deactivate")

	ri, err := rglt.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)

	// Check state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActive, ri.State)
}

func TestDeactivateErrorSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	exp := c.Now().Add(10 * time.Minute)
	setupDeactivate(rglt, m, exp)

	// Deactivate will block, then return an error.
	n.erDeactivate = errors.New("error from Deactivate")
	n.wgDeactivate.Add(1)

	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDeactivating, ri.State)
	}

	called := atomic.LoadUint32(&n.nDeactivate)
	assert.Equal(t, uint32(1), called)

	// Unblock Deactivate.
	n.wgDeactivate.Done()

	// Wait until state returns to Ready (because Deactivate returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsActive
	}, waitFor, tick)
}

func setupDrop(infos map[api.RangeID]*api.RangeInfo, m api.Meta) {
	infos[m.Ident] = &api.RangeInfo{
		Meta:  m,
		State: api.NsInactive,
	}
}

func TestDropFast(t *testing.T) {
	_, _, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	ri, err := rglt.drop(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsNotFound, ri.State)

	// Check range was successfully deleted.
	assert.NotContains(t, rglt.info, m.Ident)

	// Check idempotency.
	ri, err = rglt.drop(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsNotFound, ri.State)
}

func TestDropSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	// Drop will block.
	n.wgDrop.Add(1)

	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.drop(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDropping, ri.State)
	}

	called := atomic.LoadUint32(&n.nDrop)
	assert.Equal(t, uint32(1), called)

	// Unblock Drop.
	n.wgDrop.Done()

	// Wait until range vanishes.
	require.Eventually(t, func() bool {
		_, ok := rglt.rangeInfo(m.Ident)
		return !ok
	}, waitFor, tick)

	for i := 0; i < 2; i++ {
		ri, err := rglt.drop(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsNotFound, ri.State)
	}
}

func TestDropUnknown(t *testing.T) {
	_, _, rglt := Setup()

	ri, err := rglt.drop(1)
	require.NoError(t, err)
	assert.Equal(t,
		api.RangeInfo{
			Meta:  api.Meta{Ident: 1},
			State: api.NsNotFound,
		}, ri)
}

func TestDropErrorFast(t *testing.T) {
	_, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	n.erDrop = errors.New("error from Drop")

	ri, err := rglt.drop(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestDropErrorSlow(t *testing.T) {
	c, n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	// Drop will block, then return an error.
	n.erDrop = errors.New("error from Drop")
	n.wgDrop.Add(1)

	for i := 0; i < 2; i++ {
		if i == 0 {
			blockThenAdvance(t, c)
		}
		ri, err := rglt.drop(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDropping, ri.State)
	}

	called := atomic.LoadUint32(&n.nDrop)
	assert.Equal(t, uint32(1), called)

	// Unblock Drop.
	n.wgDrop.Done()

	// Wait until state returns to Deactivated (because Drop returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsInactive
	}, waitFor, tick)
}

// ---- helpers

// blockThenAdvance starts a goroutine which blocks until the given clock has
// one waiter, and then advances three seconds to unblock it. This is used to
// synchronize the tricky Slow tests, which block the main thread for some
// period (waiting for the timeout) before returning.
func blockThenAdvance(t *testing.T, c clockwork.FakeClock) {
	wg := sync.WaitGroup{}

	// Register a cleanup function at the end of the test, to make sure that
	// this call was actually needed. Otherwise the goroutine below may just
	// block at BlockUntil and then be terminated at the end of the test,
	// leading to a misleading test.
	t.Cleanup(func() {
		wg.Wait()
	})

	wg.Add(1)
	go func() {
		c.BlockUntil(1)
		c.Advance(3 * time.Second)
		wg.Done()
	}()
}

// ---- mocks

type MockNode struct {
	erPrepare error           // error to return
	wgPrepare *sync.WaitGroup // wg to wait before returning
	nPrepare  uint32          // call counter

	erActivate error
	wgActivate *sync.WaitGroup
	nActivate  uint32

	erDeactivate error
	wgDeactivate *sync.WaitGroup
	nDeactivate  uint32

	erDrop error
	wgDrop *sync.WaitGroup
	nDrop  uint32
}

func (n *MockNode) Prepare(m api.Meta, p []api.Parent) error {
	atomic.AddUint32(&n.nPrepare, 1)
	n.wgPrepare.Wait()
	return n.erPrepare
}

func (n *MockNode) Activate(rID api.RangeID) error {
	atomic.AddUint32(&n.nActivate, 1)
	n.wgActivate.Wait()
	return n.erActivate
}

func (n *MockNode) Deactivate(rID api.RangeID) error {
	atomic.AddUint32(&n.nDeactivate, 1)
	n.wgDeactivate.Wait()
	return n.erDeactivate
}

func (n *MockNode) Drop(rID api.RangeID) error {
	atomic.AddUint32(&n.nDrop, 1)
	n.wgDrop.Wait()
	return n.erDrop
}

func (n *MockNode) GetLoadInfo(rID api.RangeID) (api.LoadInfo, error) {
	return api.LoadInfo{}, errors.New("not implemented")
}
