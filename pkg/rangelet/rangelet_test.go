package rangelet

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// For assert.Eventually
const waitFor = 500 * time.Millisecond
const tick = 10 * time.Millisecond

func Setup() (*MockNode, *Rangelet) {
	n := &MockNode{
		wgPrepare:    &sync.WaitGroup{},
		wgAddRange:   &sync.WaitGroup{},
		wgDeactivate: &sync.WaitGroup{},
		wgDrop:       &sync.WaitGroup{},
	}

	stor := fake_storage.NewFakeStorage(nil)
	rglt := newRangelet(n, stor)
	rglt.gracePeriod = 10 * time.Millisecond
	return n, rglt
}

func TestPrepareFast(t *testing.T) {
	_, rglt := Setup()

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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// Prepare will block.
	n.wgPrepare.Add(1)

	for i := 0; i < 2; i++ {
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
	n, rglt := Setup()
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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// Prepare will block, then return an error.
	n.erPrepare = errors.New("error from Prepare")
	n.wgPrepare.Add(1)

	// Prepare the range. Even though the client will eventually return error
	// from Prepare, the outer call (give succeeds because it will exceed the
	// grace period and respond with NsPreparing.
	for i := 0; i < 2; i++ {
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
	_, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	ri, err := rglt.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)

	// Check state became NsActive.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActive, ri.State)

	// Check idempotency.
	ri, err = rglt.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)
}

func TestServeSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	// AddRange will take a long time!
	n.wgAddRange.Add(1)

	// This one will give up waiting and return early.
	ri, err := rglt.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActivating, ri.State)

	// Check that state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActivating, ri.State)

	// Unblock AddRange.
	n.wgAddRange.Done()

	// Wait until state is returned to NsActive.
	assert.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsActive
	}, waitFor, tick)

	for i := 0; i < 2; i++ {
		ri, err = rglt.serve(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsActive, ri.State)
	}
}

func TestServeUnknown(t *testing.T) {
	_, rglt := Setup()

	ri, err := rglt.serve(1)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Serve unknown range: 1")
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestServeErrorFast(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	n.erAddRange = errors.New("error from AddRange")

	ri, err := rglt.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// State was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestServeErrorSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupServe(rglt.info, m)

	// AddRange will block, then return an error.
	n.erAddRange = errors.New("error from AddRange")
	n.wgAddRange.Add(1)

	for i := 0; i < 2; i++ {
		ri, err := rglt.serve(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsActivating, ri.State)
	}

	called := atomic.LoadUint32(&n.nAddRange)
	assert.Equal(t, uint32(1), called)

	// Unblock AddRange.
	n.wgAddRange.Done()

	// Wait until state returns to Prepared (because AddRange returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsInactive
	}, waitFor, tick)
}

func setupDeactivate(infos map[api.RangeID]*api.RangeInfo, m api.Meta) {
	infos[m.Ident] = &api.RangeInfo{
		Meta:  m,
		State: api.NsActive,
	}
}

func TestDeactivateFast(t *testing.T) {
	_, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDeactivate(rglt.info, m)

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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDeactivate(rglt.info, m)

	n.wgDeactivate.Add(1)

	// Try to call serve a few times.
	// Should be the same response.
	for i := 0; i < 2; i++ {
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

func TestDeactivateUnknown(t *testing.T) {
	_, rglt := Setup()

	ri, err := rglt.take(1)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Deactivate unknown range: 1")
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestDeactivateErrorFast(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDeactivate(rglt.info, m)

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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDeactivate(rglt.info, m)

	// Deactivate will block, then return an error.
	n.erDeactivate = errors.New("error from Deactivate")
	n.wgDeactivate.Add(1)

	for i := 0; i < 2; i++ {
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
	_, rglt := Setup()

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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	// Drop will block.
	n.wgDrop.Add(1)

	for i := 0; i < 2; i++ {
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
	_, rglt := Setup()

	ri, err := rglt.drop(1)
	require.NoError(t, err)
	assert.Equal(t,
		api.RangeInfo{
			Meta:  api.Meta{Ident: 1},
			State: api.NsNotFound,
		}, ri)
}

func TestDropErrorFast(t *testing.T) {
	n, rglt := Setup()

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
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupDrop(rglt.info, m)

	// Drop will block, then return an error.
	n.erDrop = errors.New("error from Drop")
	n.wgDrop.Add(1)

	for i := 0; i < 2; i++ {
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

// ----

type MockNode struct {
	erPrepare error           // error to return
	wgPrepare *sync.WaitGroup // wg to wait before returning
	nPrepare  uint32          // call counter

	erAddRange error
	wgAddRange *sync.WaitGroup
	nAddRange  uint32

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

func (n *MockNode) AddRange(rID api.RangeID) error {
	atomic.AddUint32(&n.nAddRange, 1)
	n.wgAddRange.Wait()
	return n.erAddRange
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
