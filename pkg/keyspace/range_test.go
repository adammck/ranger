package keyspace

import (
	"testing"

	"github.com/adammck/ranger/pkg/keyspace/fsm"
	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {

	// Whole keyspace by default
	r := Range{}
	assert.Equal(t, "{0 Pending [-inf, +inf]}", r.String())

	// This makes no sense; b>a, so no keys can be in this range
	r = Range{start: "b", end: "a"}
	assert.Equal(t, "{0 Pending (b, a]}", r.String())

	r = Range{start: "a", end: "b"}
	assert.Equal(t, "{0 Pending (a, b]}", r.String())
}

func TestState(t *testing.T) {
	r := Range{}

	err := r.State(fsm.Pending)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "invalid state transition: Pending -> Pending")
	}

	// TODO
}

func TestSplitState(t *testing.T) {
	r0 := Range{state: fsm.Splitting}
	r1 := Range{state: fsm.Pending, parents: []*Range{&r0}}
	r2 := Range{state: fsm.Pending, parents: []*Range{&r0}}

	r0.children = []*Range{&r1, &r2}

	assert.NoError(t, r1.State(fsm.Ready))
	assert.Equal(t, r0.state, fsm.Splitting)

	assert.NoError(t, r2.State(fsm.Ready))
	assert.Equal(t, r0.state, fsm.Obsolete)
}
