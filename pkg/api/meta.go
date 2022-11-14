package api

import (
	"fmt"
)

// Meta is a range minus all the state.
// Should be immutable after construction.
// TODO: Rename this to RangeMeta.
type Meta struct {
	Ident RangeID
	Start Key // inclusive
	End   Key // exclusive
}

// String returns a string like: 1234 (aaa, bbb]
func (m Meta) String() string {
	var s, e string

	if m.Start == ZeroKey {
		s = "[-inf"
	} else {
		s = fmt.Sprintf("(%s", m.Start)
	}

	if m.End == ZeroKey {
		e = "+inf]"
	} else {
		e = fmt.Sprintf("%s]", m.End)
	}

	return fmt.Sprintf("%s %s, %s", m.Ident.String(), s, e)
}

func (m *Meta) Contains(k Key) bool {
	if m.Start != ZeroKey {
		if k < m.Start {
			return false
		}
	}

	if m.End != ZeroKey {
		// Note that the range end is exclusive!
		if k >= m.End {
			return false
		}
	}

	return true
}
