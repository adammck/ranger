package balancer

import "github.com/adammck/ranger/pkg/ranje"

type MoveRequest struct {
	Range ranje.Ident
	Node  string
}

type JoinRequest struct {
	Left  ranje.Ident
	Right ranje.Ident
	Node  string
}

type SplitRequest struct {
	Range     ranje.Ident
	Boundary  ranje.Key
	NodeLeft  string
	NodeRight string
}
