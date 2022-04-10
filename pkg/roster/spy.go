package roster

import "github.com/adammck/ranger/pkg/ranje"

type RpcType uint8

const (
	Give RpcType = iota
	Serve
	Take
	Drop
)

type RpcRecord struct {
	Type  RpcType
	Node  string
	Range ranje.Ident
}

func (rt *RpcType) String() string {
	switch *rt {
	case Give:
		return "Give"
	case Serve:
		return "Serve"
	case Take:
		return "Take"
	case Drop:
		return "Drop"
	}

	// Probably a state was added but this method wasn't updated.
	panic("unknown RpcType value!")
}
