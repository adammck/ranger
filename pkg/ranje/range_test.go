package ranje

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {

	// Whole keyspace by default
	r := Range{}
	assert.Equal(t, "{0 Pending [-inf, +inf]}", r.String())

	// This makes no sense; b>a, so no keys can be in this range
	r = Range{Meta: Meta{Start: "b", End: "a"}}
	assert.Equal(t, "{0 Pending (b, a]}", r.String())

	r = Range{Meta: Meta{Start: "a", End: "b"}}
	assert.Equal(t, "{0 Pending (a, b]}", r.String())
}

func TestState(t *testing.T) {
	r := Range{}

	err := r.ToState(Pending)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "invalid state transition: Pending -> Pending")
	}

	// TODO
}

func TestSplitState(t *testing.T) {
	r0 := Range{State: Splitting}
	r1 := Range{State: Pending, parents: []*Range{&r0}}
	r2 := Range{State: Pending, parents: []*Range{&r0}}

	r0.children = []*Range{&r1, &r2}

	assert.NoError(t, r1.ToState(Ready))
	assert.Equal(t, r0.State, Splitting)

	assert.NoError(t, r2.ToState(Ready))
	assert.Equal(t, r0.State, Obsolete)
}
