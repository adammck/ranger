package roster

import (
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
)

// See: ranger/pkg/proto/node.proto:RangeInfo
type Info struct {
	R *keyspace.Range
	S ranje.RemoteState

	// The number of keys that the range has.
	K uint64
}
