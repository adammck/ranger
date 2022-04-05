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
