package mock

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
)

type Command struct {
	rID ranje.Ident // sort by this
	nID string      // and then this
	act api.Action
}

func (t *Command) String() string {
	return fmt.Sprintf("%s(R%d, %s)", t.act, t.rID, t.nID)
}

func (t *Command) Less(other Command) bool {
	if t.rID != other.rID {
		return t.rID < other.rID
	} else {
		return t.nID < other.nID
	}
}
