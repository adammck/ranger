package orchestrator

import (
	"github.com/adammck/ranger/pkg/ranje"
)

// TODO: Split this into Add, Remove
type OpMove struct {
	Range ranje.Ident
	Src   string
	Dest  string
	Err   chan error
}

type OpSplit struct {
	Range ranje.Ident
	Key   ranje.Key
	Left  string
	Right string
	Err   chan error
}

type OpJoin struct {
	Left  ranje.Ident
	Right ranje.Ident
	Dest  string
	Err   chan error
}
