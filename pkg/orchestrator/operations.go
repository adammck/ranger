package orchestrator

import "github.com/adammck/ranger/pkg/api"

// TODO: Split this into Add, Remove
type OpMove struct {
	Range api.Ident
	Src   string
	Dest  string
	Err   chan error
}

type OpSplit struct {
	Range api.Ident
	Key   api.Key
	Left  string
	Right string
	Err   chan error
}

type OpJoin struct {
	Left  api.Ident
	Right api.Ident
	Dest  string
	Err   chan error
}
