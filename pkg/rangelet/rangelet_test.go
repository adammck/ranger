package rangelet

import (
	"errors"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_node"
	"github.com/stretchr/testify/assert"
)

type MockNode struct {
	rGive  error
	rServe error
	rTake  error
	rDrop  error
}

func (n *MockNode) PrepareAddRange(m ranje.Meta, p []api.Parent) error {
	return n.rGive
}

func (n *MockNode) AddRange(rID ranje.Ident) error {
	return n.rServe
}

func (n *MockNode) PrepareDropRange(rID ranje.Ident) error {
	return n.rTake
}

func (n *MockNode) DropRange(rID ranje.Ident) error {
	return n.rDrop
}

func Setup() (*MockNode, *Rangelet) {
	n := &MockNode{}
	stor := fake_node.NewStorage(nil)
	rglt := NewRangelet(n, nil, stor)
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

	// correct state to become ready.
	r.info[m.Ident] = &info.RangeInfo{
		Meta:  m,
		State: state.NsPrepared,
	}

	ri, err = r.serve(m.Ident)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsReadying, ri.State)
	}

	// TODO: Noooo
	time.Sleep(10 * time.Millisecond)

	ri, err = r.serve(m.Ident)
	if assert.NoError(t, err) {
		assert.Equal(t, m, ri.Meta)
		assert.Equal(t, state.NsReady, ri.State)
	}
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
