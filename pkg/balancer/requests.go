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
	Range     ranje.Ident
	SrcNodeID string
	Node      string // rename to DestNodeID
}

func (req MoveRequest) Run(b *Balancer) {

	// TODO: Lock ks.ranges!
	r, err := b.ks.GetByIdent(req.Range)
	if err != nil {
		fmt.Printf("Move failed: %s\n", err.Error())
		return
	}

	nSrc := b.rost.NodeByIdent(req.SrcNodeID)
	if nSrc == nil {
		fmt.Printf("Move failed: Src node not found: %s\n", req.SrcNodeID)
		return
	}

	n := b.rost.NodeByIdent(req.Node)
	if n == nil {
		fmt.Printf("Move failed: No such node: %s\n", req.Node)
		return
	}

	operations.Move(r, nSrc, n)
}

type JoinRequest struct {
	RangeLeft  ranje.Ident
	RangeRight ranje.Ident
	NodeDst    string
}

func (req JoinRequest) Run(b *Balancer) {
	// TODO: Do these lookups in the operation itself, so they can be lazy and resumable.

	// TODO: Lock ks.ranges!
	rLeft, err := b.ks.GetByIdent(req.RangeLeft)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	// TODO: Lock ks.ranges!
	rRight, err := b.ks.GetByIdent(req.RangeRight)
	if err != nil {
		fmt.Printf("Join failed: %s\n", err.Error())
		return
	}

	node := b.rost.NodeByIdent(req.NodeDst)
	if node == nil {
		fmt.Printf("Join failed: No such node: %s\n", req.NodeDst)
		return
	}

	op := operations.JoinOp{
		Keyspace:      b.ks,
		Roster:        b.rost,
		RangeSrcLeft:  rLeft.Meta.Ident,
		RangeSrcRight: rRight.Meta.Ident,
		NodeDst:       node.Ident(),
	}

	op.Run()
}

type SplitRequest struct {
	Range     ranje.Ident
	Boundary  ranje.Key
	NodeLeft  string
	NodeRight string
}

func (req SplitRequest) Run(b *Balancer) {
	op := operations.SplitOp{
		Keyspace:     b.ks,
		Roster:       b.rost,
		RangeSrc:     req.Range,
		Boundary:     req.Boundary,
		NodeDstLeft:  req.NodeLeft,
		NodeDstRight: req.NodeRight,
	}

	op.Run()
}
