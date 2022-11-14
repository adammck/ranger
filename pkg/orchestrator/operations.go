package orchestrator

import "github.com/adammck/ranger/pkg/api"

// TODO: Split this into Add, Remove
type OpMove struct {
	Range api.RangeID
	Src   string
	Dest  string
	Err   chan error
}

type OpSplit struct {
	Range api.RangeID
	Key   api.Key
	Left  string
	Right string
	Err   chan error
}

type OpJoin struct {
	Left  api.RangeID
	Right api.RangeID
	Dest  string
	Err   chan error
}
