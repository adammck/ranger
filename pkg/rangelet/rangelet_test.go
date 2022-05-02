package rangelet

import (
	"errors"
	"sync"
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
	rGive   error
	rServe  error
	wgServe *sync.WaitGroup
	rTake   error
	rDrop   error
}

func (n *MockNode) PrepareAddRange(m ranje.Meta, p []api.Parent) error {
	return n.rGive
}

func (n *MockNode) AddRange(rID ranje.Ident) error {
	if n.wgServe != nil {
		n.wgServe.Wait()
	}
	return n.rServe
}

func (n *MockNode) PrepareDropRange(rID ranje.Ident) error {
	return n.rTake
}

func (n *MockNode) DropRange(rID ranje.Ident) error {
	return n.rDrop
}

func (n *MockNode) GetLoadInfo(rID ranje.Ident) (api.LoadInfo, error) {
	return api.LoadInfo{}, errors.New("not implemented")
}

func Setup() (*MockNode, *Rangelet) {
	n := &MockNode{}
	stor := fake_storage.NewFakeStorage(nil)
	rglt := NewRangelet(n, nil, stor)
	rglt.gracePeriod = 10 * time.Millisecond
	return n, rglt
}

func TestGiveError(t *testing.T) {
	n, r := Setup()
	n.rGive = errors.New("error from PrepareAddRange")

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	// Give the range. Even though the client will return error from
	// PrepareAddRange, this will succeed because we call that method in the
	// background for now. The controller only learns about the failure (or
	// success!) next time it gives or probes.
	ri, err := r.give(m, p)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsPreparing, ri.State)
	}

	// This is just to give the background goroutine (which actually calls
	// PrepareAddRange) plenty of time to do its thing. Oh dear.
	//
	// TODO: Remove this! Change the rangelet to wait for a bit before
	//       backgrounding the PrepareAddRange (and everything else) so the
	//       simple/fast case can be synchronous.
	time.Sleep(10 * time.Millisecond)

	// Send the same request again.
	ri, err = r.give(m, p)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsPreparingError, ri.State)
	}
}

func TestGiveSuccess(t *testing.T) {
	_, r := Setup()

	m := ranje.Meta{Ident: 1}
	p := []api.Parent{}

	ri, err := r.give(m, p)
	if assert.NoError(t, err) {
		assert.Equal(t, ri.Meta, m)
		assert.Equal(t, ri.State, state.NsPreparing)
	}

	// TODO: Noooo
	time.Sleep(10 * time.Millisecond)

	// Send the same request again.
	ri, err = r.give(m, p)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsPrepared, ri.State)
	}
}

func TestServe(t *testing.T) {
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

func TestSlowServe(t *testing.T) {
	n, r := Setup()

	m := ranje.Meta{Ident: 1}

	ri, err := r.serve(m.Ident)
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Serve unknown range: 1")

	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	// AddRange will take a long time!
	n.wgServe = &sync.WaitGroup{}
	n.wgServe.Add(1)

	// This one will give up waiting and return early.
	ri, err = r.serve(m.Ident)
	require.NoError(t, err)
	assert.Equal(t, m, ri.Meta)
	assert.Equal(t, state.NsReadying, ri.State)

	// Check that state was updated.
	ri, ok := r.rangeInfo(m.Ident)
	require.True(t, ok)
	assert.Equal(t, state.NsReadying, ri.State)

	// AddRange finished.
	n.wgServe.Done()

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

func TestTake(t *testing.T) {
	_, r := Setup()

	m := ranje.Meta{Ident: 1}

	ri, err := r.take(m.Ident)
	assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = can't Take unknown range: 1")

	// correct state to be taken.
	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsReady,
	}

	ri, err = r.take(m.Ident)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsTaking, ri.State)
	}

	// TODO: Noooo
	time.Sleep(10 * time.Millisecond)

	ri, err = r.take(m.Ident)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsTaken, ri.State)
	}
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
