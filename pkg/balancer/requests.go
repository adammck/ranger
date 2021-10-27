package balancer

import (
	"fmt"

	"github.com/adammck/ranger/pkg/operations"
	"github.com/adammck/ranger/pkg/ranje"
)

type OpRunner interface {
	Run(b *Balancer)
}

type MoveRequest struct {
	Range ranje.Ident
	Node  string
}

func (req MoveRequest) Run(b *Balancer) {

	// TODO: Lock ks.ranges!
	r, err := b.ks.GetByIdent(req.Range)
	if err != nil {
		fmt.Printf("Move failed: %s\n", err.Error())
		return
	}

	n := b.rost.NodeByIdent(req.Node)
	if n == nil {
		fmt.Printf("Move failed: No such node: %s\n", req.Node)
		return
	}

	operations.Move(r, n)
}

type JoinRequest struct {
	Left  ranje.Ident
	Right ranje.Ident
	Node  string
}

func (req JoinRequest) Run(b *Balancer) {

	// TODO: Lock ks.ranges!
	r1, err := b.ks.GetByIdent(req.Left)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	// TODO: Lock ks.ranges!
	r2, err := b.ks.GetByIdent(req.Right)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	node := b.rost.NodeByIdent(req.Node)
	if node == nil {
		fmt.Printf("Join failed: No such node: %s\n", req.Node)
		return
	}

	operations.Join(b.ks, r1, r2, node)
}

type SplitRequest struct {
	Range     ranje.Ident
	Boundary  ranje.Key
	NodeLeft  string
	NodeRight string
}

func (req SplitRequest) Run(b *Balancer) {

	nLeft := b.rost.NodeByIdent(req.NodeLeft)
	if nLeft == nil {
		fmt.Printf("Split failed: No such node (left): %s\n", req.NodeLeft)
		return
	}

	nRight := b.rost.NodeByIdent(req.NodeRight)
	if nRight == nil {
		fmt.Printf("Split failed: No such node (right): %s\n", req.NodeRight)
		return
	}

	// TODO: Lock ks.ranges!
	r, err := b.ks.GetByIdent(req.Range)
	if err != nil {
		fmt.Printf("Split failed: %s\n", err.Error())
		return
	}

	operations.Split(b.ks, r, req.Boundary, nLeft, nRight)
}
