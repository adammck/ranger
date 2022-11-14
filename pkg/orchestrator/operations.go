package orchestrator

import "github.com/adammck/ranger/pkg/api"

// TODO: Split this into Add, Remove
type OpMove struct {
	Range api.RangeID
	Src   api.NodeID
	Dest  api.NodeID
	Err   chan error
}

type OpSplit struct {
	Range api.RangeID
	Key   api.Key
	Left  api.NodeID
	Right api.NodeID
	Err   chan error
}

type OpJoin struct {
	Left  api.RangeID
	Right api.RangeID
	Dest  api.NodeID
	Err   chan error
}
