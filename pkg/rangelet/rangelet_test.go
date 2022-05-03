package rangelet

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// For assert.Eventually
const waitFor = 500 * time.Millisecond
const tick = 10 * time.Millisecond

type MockNode struct {
	erPrepareAddRange error
	wgPrepareAddRange *sync.WaitGroup
	nPrepareAddRange  uint32

	erAddRange error
	wgAddRange *sync.WaitGroup
	nAddRange  uint32

	erPrepareDropRange error
	wgPrepareDropRange *sync.WaitGroup
	nPrepareDropRange  uint32

	rDrop error
}

func (n *MockNode) PrepareAddRange(m ranje.Meta, p []api.Parent) error {
	atomic.AddUint32(&n.nPrepareAddRange, 1)
	n.wgPrepareAddRange.Wait()
	return n.erPrepareAddRange
}

func (n *MockNode) AddRange(rID ranje.Ident) error {
	atomic.AddUint32(&n.nAddRange, 1)
	n.wgAddRange.Wait()
	return n.erAddRange
}

func (n *MockNode) PrepareDropRange(rID ranje.Ident) error {
	atomic.AddUint32(&n.nPrepareDropRange, 1)
	n.wgPrepareDropRange.Wait()
	return n.erPrepareDropRange
}

func (n *MockNode) DropRange(rID ranje.Ident) error {
	return n.rDrop
}

func (n *MockNode) GetLoadInfo(rID ranje.Ident) (api.LoadInfo, error) {
	return api.LoadInfo{}, errors.New("not implemented")
}

func Setup() (*MockNode, *Rangelet) {
	n := &MockNode{
		wgPrepareAddRange:  &sync.WaitGroup{},
		wgAddRange:         &sync.WaitGroup{},
		wgPrepareDropRange: &sync.WaitGroup{},
	}

	stor := fake_storage.NewFakeStorage(nil)
	rglt := NewRangelet(n, nil, stor)
	rglt.gracePeriod = 10 * time.Millisecond
	return n, rglt
}

func TestGiveErrorFast(t *testing.T) {
	n, rglt := Setup()
	n.erPrepareAddRange = errors.New("error from PrepareAddRange")

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsPreparingError, ri.State)

	// TODO: Remove this! It's totally wrong!
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsPreparingError, ri.State)

	// TODO: This is how it should work!
	// // Check that no range was created.
	// ri, ok := rglt.rangeInfo(m.Ident)
	// assert.False(t, ok)
	// assert.Equal(t, info.RangeInfo{}, ri)
}

func TestGiveErrorSlow(t *testing.T) {
	n, rglt := Setup()

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	// PrepareAddRange will block, then return an error.
	n.erPrepareAddRange = errors.New("error from PrepareAddRange")
	n.wgPrepareAddRange.Add(1)

	// Give the range. Even though the client will eventually return error from
	// PrepareAddRange, the outer call (give succeeds because it will exceed the
	// grace period and respond with Preparing.
	for i := 0; i < 2; i++ {
		ri, err := rglt.give(m, p)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsPreparing, ri.State)
	}

	// Subsequent gives should have deduped.
	called := atomic.LoadUint32(&n.nPrepareAddRange)
	assert.Equal(t, uint32(1), called)

	// PrepareAddRange finished.
	n.wgPrepareAddRange.Done()

	// Wait until PreparingError (because PrepareAddRange returned error)
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == state.NsPreparingError
	}, waitFor, tick)

	// Send the same request again, this time expecting error.
	for i := 0; i < 2; i++ {
		ri, err := rglt.give(m, p)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsPreparingError, ri.State)
	}
}

func TestGiveSuccessFast(t *testing.T) {
	_, rglt := Setup()

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsPrepared, ri.State)

	// Check that range was created in the expected state.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsPrepared, ri.State)
}

func TestGiveSuccessSlow(t *testing.T) {
	n, rglt := Setup()

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	// PrepareAddRange will block.
	n.wgPrepareAddRange.Add(1)

	// This will wait for grace period then return early.
	ri, err := rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, ri.Meta, m)
	assert.Equal(t, state.NsPreparing, ri.State)

	// Check range is Preparing (because PrepareAddRange is running)
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsPreparing, ri.State)

	// AddRange finished.
	n.wgPrepareAddRange.Done()

	// Wait until state becomes Prepared (because PrepareAddRange finished)
	assert.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == state.NsPrepared
	}, waitFor, tick)

	// Send the request again.
	ri, err = rglt.give(m, p)
	require.NoError(t, err)
	assert.Equal(t, ri.Meta, m)
	assert.Equal(t, state.NsPrepared, ri.State)
}

func TestServeFast(t *testing.T) {
	_, r := Setup()

	m := ranje.Meta{Ident: 1}

	ri, err := r.serve(m.Ident)
	assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Serve unknown range: 1")

	// Add a range in ready-to-serve state.
	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	ri, err = r.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsReady, ri.State)

	// Check that state was updated.
	ri, ok := r.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsReady, ri.State)
}

func TestServeSlow(t *testing.T) {
	n, r := Setup()

	m := ranje.Meta{Ident: 1}

	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	// AddRange will take a long time!
	n.wgAddRange.Add(1)

	// This one will give up waiting and return early.
	ri, err := r.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsReadying, ri.State)

	// Check that state was updated.
	ri, ok := r.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsReadying, ri.State)

	// AddRange finished.
	n.wgAddRange.Done()

	// Wait until state is updated
	assert.Eventually(t, func() bool {
		ri, ok := r.rangeInfo(m.Ident)
		return ok && ri.State == state.NsReady
	}, waitFor, tick)

	ri, err = r.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsReady, ri.State)
}

func TestServeErrorFast(t *testing.T) {
	n, rglt := Setup()
	n.erAddRange = errors.New("error from AddRange")

	m := ranje.Meta{Ident: 1}

	// Valid state to receive Serve.
	rglt.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	ri, err := rglt.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsReadyingError, ri.State)

	// State was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsReadyingError, ri.State)
}

func TestServeErrorSlow(t *testing.T) {
	n, rglt := Setup()

	m := ranje.Meta{Ident: 1}

	// Valid state to receive Serve.
	rglt.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	// AddRange will block, then return an error.
	n.erAddRange = errors.New("error from AddRange")
	n.wgAddRange.Add(1)

	// Try to call serve a few times.
	// Should be the same response.
	for i := 0; i < 2; i++ {
		ri, err := rglt.serve(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsReadying, ri.State)
	}

	// Subsequent gives should have deduped.
	called := atomic.LoadUint32(&n.nAddRange)
	assert.Equal(t, uint32(1), called)

	// AddRange finished.
	n.wgAddRange.Done()

	// Wait until ReadyingError (because AddRange returned error).
	require.Eventually(t, func() bool {
		ri, ok := rglt.rangeInfo(m.Ident)
		return ok && ri.State == state.NsReadyingError
	}, waitFor, tick)

	// Send the same request again, this time expecting error.
	for i := 0; i < 2; i++ {
		ri, err := rglt.serve(m.Ident)
		require.Error(t, err)
		assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid state for Serve: NsReadyingError")
		assert.Equal(t, state.NsReadyingError, ri.State)
	}
}

func TestTakeFast(t *testing.T) {
	_, r := Setup()

	m := ranje.Meta{Ident: 1}

	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsReady,
	}

	ri, err := r.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsTaken, ri.State)

	ri, ok := r.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsTaken, ri.State)
}

func TestTakeSlow(t *testing.T) {
	n, r := Setup()

	m := ranje.Meta{Ident: 1}

	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsReady,
	}

	n.wgPrepareDropRange.Add(1)

	// Try to call serve a few times.
	// Should be the same response.
	for i := 0; i < 2; i++ {
		ri, err := r.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsTaking, ri.State)
	}

	// Check that state was updated.
	ri, ok := r.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsTaking, ri.State)

	// PrepareDropRange finished.
	n.wgPrepareDropRange.Done()

	// Wait until state is updated.
	assert.Eventually(t, func() bool {
		ri, ok := r.rangeInfo(m.Ident)
		return ok && ri.State == state.NsTaken
	}, waitFor, tick)

	for i := 0; i < 2; i++ {
		ri, err := r.take(m.Ident)
		require.NoError(t, err)
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsTaken, ri.State)
	}
}

func TestTakeErrorFast(t *testing.T) {
	n, rglt := Setup()
	n.erPrepareDropRange = errors.New("error from PrepareDropRange")

	m := ranje.Meta{Ident: 1}

	// Valid for PrepareDropRange.
	rglt.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsReady,
	}

	ri, err := rglt.take(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsTakingError, ri.State)

	// Check state was updated.
	ri, ok := rglt.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsTakingError, ri.State)
}

func TestDrop(t *testing.T) {
	_, r := Setup()

	m := ranje.Meta{Ident: 1}

	ri, err := r.drop(m.Ident)
	assert.ErrorIs(t, err, ErrNotFound)

	// correct state to be dropn.
	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsTaken,
	}

	ri, err = r.drop(m.Ident)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsDropping, ri.State)
	}

	// TODO: Noooo
	time.Sleep(10 * time.Millisecond)

	ri, err = r.drop(m.Ident)
	if assert.ErrorIs(t, err, ErrNotFound) {
		// The range was successfully deleted.
		assert.NotContains(t, r.info, m.Ident)
	}
}
