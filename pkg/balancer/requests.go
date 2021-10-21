package balancer

import "github.com/adammck/ranger/pkg/ranje"

type JoinRequest struct {
	Left  ranje.Ident
	Right ranje.Ident
	Node  string
}
