package ranje

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type FakeKeyspace struct{}

func (ks *FakeKeyspace) Get(id Ident) (*Range, error) {
	panic("not implemented")
}

func TestString(t *testing.T) {

	// Whole keyspace by default
	r := Range{}
	assert.Equal(t, "{0 [-inf, +inf] Unknown}", r.LogString())

	// This makes no sense; b>a, so no keys can be in this range
	r = Range{Meta: Meta{Start: "b", End: "a"}}
	assert.Equal(t, "{0 (b, a] Unknown}", r.LogString())

	r = Range{Meta: Meta{Start: "a", End: "b"}}
	assert.Equal(t, "{0 (a, b] Unknown}", r.LogString())
}

func TestState(t *testing.T) {
	ks := &FakeKeyspace{}
	r := Range{}

	err := r.toState(Pending, ks)
	if assert.Error(t, err) {
		assert.EqualError(t, err, "invalid range state transition: Unknown -> Pending")
	}

	// TODO
}

func TestSplitState(t *testing.T) {
	ks := &FakeKeyspace{}
	p := &FakePersister{}
	r1 := Range{pers: p, State: Splitting}
	r2 := Range{pers: p, State: Pending, Parents: []Ident{r1.Meta.Ident}}
	r3 := Range{pers: p, State: Pending, Parents: []Ident{r1.Meta.Ident}}
	r1.Children = []Ident{r2.Meta.Ident, r3.Meta.Ident}

	assert.NoError(t, r2.toState(Placing, ks))
	assert.NoError(t, r2.toState(Ready, ks))
	assert.Equal(t, r1.State, Splitting)

	assert.NoError(t, r3.toState(Placing, ks))
	assert.NoError(t, r3.toState(Ready, ks))

	assert.NoError(t, r1.toState(Obsolete, ks))
	assert.Equal(t, r1.State, Obsolete)
}
