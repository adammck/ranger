package mock

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

// TODO: Move this into API. (But need to move ranje.Ident first.)
type Command struct {
	rID api.Ident // sort by this
	nID string    // and then this
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
