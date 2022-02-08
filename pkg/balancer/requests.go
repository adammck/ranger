package balancer

import (
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
	op := operations.MoveOp{
		Keyspace: b.ks,
		Roster:   b.rost,
		RangeSrc: req.Range,
		NodeDst:  req.Node,
	}

	op.Run()
}

type JoinRequest struct {
	RangeLeft  ranje.Ident
	RangeRight ranje.Ident
	NodeDst    string
}

func (req JoinRequest) Run(b *Balancer) {
	op := operations.JoinOp{
		Keyspace:      b.ks,
		Roster:        b.rost,
		RangeSrcLeft:  req.RangeLeft,
		RangeSrcRight: req.RangeRight,
		NodeDst:       req.NodeDst,
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
