package balancer

import (
	"github.com/adammck/ranger/pkg/operations/join"
	"github.com/adammck/ranger/pkg/operations/move"
	"github.com/adammck/ranger/pkg/operations/split"
	"github.com/adammck/ranger/pkg/ranje"
)

type OpRunner interface {
	Run(b *Balancer) error
}

// This file is now totally pointless indirection. Remove it.

type MoveRequest struct {
	Range ranje.Ident
	Node  string
}

func (req MoveRequest) Run(b *Balancer) error {
	return move.Run(&move.MoveOp{
		Keyspace: b.ks,
		Roster:   b.rost,
		Range:    req.Range,
		Node:     req.Node,
	})
}

type JoinRequest struct {
	RangeLeft  ranje.Ident
	RangeRight ranje.Ident
	Node       string
}

func (req JoinRequest) Run(b *Balancer) error {
	return join.Run(&join.JoinOp{
		Keyspace:   b.ks,
		Roster:     b.rost,
		RangeLeft:  req.RangeLeft,
		RangeRight: req.RangeRight,
		Node:       req.Node,
	})
}

type SplitRequest struct {
	Range     ranje.Ident
	Boundary  ranje.Key
	NodeLeft  string
	NodeRight string
}

func (req SplitRequest) Run(b *Balancer) error {
	return split.Run(&split.SplitOp{
		Keyspace:  b.ks,
		Roster:    b.rost,
		Range:     req.Range,
		Boundary:  req.Boundary,
		NodeLeft:  req.NodeLeft,
		NodeRight: req.NodeRight,
	})
}
