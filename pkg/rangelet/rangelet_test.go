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
		wgPrepareAddRange:  &sync.WaitGroup{},
		wgAddRange:         &sync.WaitGroup{},
		wgPrepareDropRange: &sync.WaitGroup{},
		wgDropRange:        &sync.WaitGroup{},
	}

	stor := fake_storage.NewFakeStorage(nil)
	rglt := NewRangelet(n, nil, stor)
	rglt.gracePeriod = 10 * time.Millisecond
	return n, rglt
}

func TestGiveFast(t *testing.T) {
	_, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check range was created.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)

	// Check idempotency.
	ri, err = rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsInactive, ri.State)
}

func TestGiveSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// PrepareAddRange will block.
	n.wgPrepareAddRange.Add(1)

	for i := 0; i < 2; i++ {
		ri, err := rglt.give(m, p)
		require.NoError(t, err)
		assert.Equal(t, ri.Meta, m)
		assert.Equal(t, api.NsLoading, ri.State)
	}

	called := atomic.LoadUint32(&n.nPrepareAddRange)
	assert.Equal(t, uint32(1), called)

	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsLoading, ri.State)

	// Unblock PrepareAddRange.
	n.wgPrepareAddRange.Done()

	// Wait until range exists.
	assert.Eventually(t, func() bool {
		_, ok := rglt.rangeInfo(m.Ident)
		return ok
	}, waitFor, tick)

	ri, ok = rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsInactive, ri.State)

	for i := 0; i < 2; i++ {
		ri, err := rglt.give(m, p)
		require.NoError(t, err)
		assert.Equal(t, ri.Meta, m)
		assert.Equal(t, api.NsInactive, ri.State)
	}
}

func TestGiveErrorFast(t *testing.T) {
	n, rglt := Setup()
	n.erPrepareAddRange = errors.New("error from PrepareAddRange")

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsNotFound, ri.State)

	// Check that no range was created.
	ri, ok := rglt.rangeInfo(m.Ident)
	assert.False(t, ok)
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestGiveErrorSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	p := []api.Parent{}

	// PrepareAddRange will block, then return an error.
	n.erPrepareAddRange = errors.New("error from PrepareAddRange")
	n.wgPrepareAddRange.Add(1)

	// Give the range. Even though the client will eventually return error from
	// PrepareAddRange, the outer call (give succeeds because it will exceed the
	// grace period and respond with Loading.
	for i := 0; i < 2; i++ {
		ri, err := rglt.give(m, p)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsLoading, ri.State)
	}

	called := atomic.LoadUint32(&n.nPrepareAddRange)
	assert.Equal(t, uint32(1), called)

	// Unblock PrepareAddRange.
	n.wgPrepareAddRange.Done()

	// Wait until range vanishes (because PrepareAddRange returned error).
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

func setupTake(infos map[api.RangeID]*api.RangeInfo, m api.Meta) {
	infos[m.Ident] = &api.RangeInfo{
		Meta:  m,
		State: api.NsActive,
	}
}

func TestTakeFast(t *testing.T) {
	_, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupTake(rglt.info, m)

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

func TestTakeSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupTake(rglt.info, m)

	n.wgPrepareDropRange.Add(1)

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

	// Unblock PrepareDropRange.
	n.wgPrepareDropRange.Done()

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

func TestTakeUnknown(t *testing.T) {
	_, rglt := Setup()

	ri, err := rglt.take(1)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Take unknown range: 1")
	assert.Equal(t, api.RangeInfo{}, ri)
}

func TestTakeErrorFast(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupTake(rglt.info, m)

	n.erPrepareDropRange = errors.New("error from PrepareDropRange")

	ri, err := rglt.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, api.NsActive, ri.State)

	// Check state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, api.NsActive, ri.State)
}

func TestTakeErrorSlow(t *testing.T) {
	n, rglt := Setup()

	m := api.Meta{Ident: 1}
	setupTake(rglt.info, m)

	// PrepareDropRange will block, then return an error.
	n.erPrepareDropRange = errors.New("error from PrepareDropRange")
	n.wgPrepareDropRange.Add(1)

	for i := 0; i < 2; i++ {
		ri, err := rglt.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDeactivating, ri.State)
	}

	called := atomic.LoadUint32(&n.nPrepareDropRange)
	assert.Equal(t, uint32(1), called)

	// Unblock PrepareDropRange.
	n.wgPrepareDropRange.Done()

	// Wait until state returns to Ready (because PrepareDropRange returned error).
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

	// DropRange will block.
	n.wgDropRange.Add(1)

	for i := 0; i < 2; i++ {
		ri, err := rglt.drop(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDropping, ri.State)
	}

	called := atomic.LoadUint32(&n.nDropRange)
	assert.Equal(t, uint32(1), called)

	// Unblock DropRange.
	n.wgDropRange.Done()

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

	n.erDropRange = errors.New("error from DropRange")

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

	// DropRange will block, then return an error.
	n.erDropRange = errors.New("error from DropRange")
	n.wgDropRange.Add(1)

	for i := 0; i < 2; i++ {
		ri, err := rglt.drop(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, api.NsDropping, ri.State)
	}

	called := atomic.LoadUint32(&n.nDropRange)
	assert.Equal(t, uint32(1), called)

	// Unblock DropRange.
	n.wgDropRange.Done()

	// Wait until state returns to Taken (because DropRange returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == api.NsInactive
	}, waitFor, tick)
}

// ----

type MockNode struct {
	erPrepareAddRange error           // error to return
	wgPrepareAddRange *sync.WaitGroup // wg to wait before returning
	nPrepareAddRange  uint32          // call counter

	erAddRange error
	wgAddRange *sync.WaitGroup
	nAddRange  uint32

	erPrepareDropRange error
	wgPrepareDropRange *sync.WaitGroup
	nPrepareDropRange  uint32

	erDropRange error
	wgDropRange *sync.WaitGroup
	nDropRange  uint32
}

func (n *MockNode) PrepareAddRange(m api.Meta, p []api.Parent) error {
	atomic.AddUint32(&n.nPrepareAddRange, 1)
	n.wgPrepareAddRange.Wait()
	return n.erPrepareAddRange
}

func (n *MockNode) AddRange(rID api.RangeID) error {
	atomic.AddUint32(&n.nAddRange, 1)
	n.wgAddRange.Wait()
	return n.erAddRange
}

func (n *MockNode) PrepareDropRange(rID api.RangeID) error {
	atomic.AddUint32(&n.nPrepareDropRange, 1)
	n.wgPrepareDropRange.Wait()
	return n.erPrepareDropRange
}

func (n *MockNode) DropRange(rID api.RangeID) error {
	atomic.AddUint32(&n.nDropRange, 1)
	n.wgDropRange.Wait()
	return n.erDropRange
}

func (n *MockNode) GetLoadInfo(rID api.RangeID) (api.LoadInfo, error) {
	return api.LoadInfo{}, errors.New("not implemented")
}
