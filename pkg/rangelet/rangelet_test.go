package rangelet

import (
	"context"
	"testing"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/test/fake_node"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func Setup() (*Rangelet, func()) {
	stor := fake_node.NewStorage(nil)
	n := fake_node.NewTestNode("nothing:8000", stor)
	srv := grpc.NewServer()
	rglt := NewRangelet(n, srv, stor)
	closer := n.Listen(context.Background(), srv)
	return rglt, closer
}

func TestGive(t *testing.T) {
	r, closer := Setup()
	defer closer()

	m := ranje.Meta{
		Ident: 1,
		Start: ranje.ZeroKey,
	}

	p := []api.Parent{}

	_, err := r.give(m, p)
	if assert.NoError(t, err) {

	}
}
