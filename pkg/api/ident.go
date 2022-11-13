package api

import (
	"fmt"
)

// Ident is the unique identity of a range. It's just a uint64.

type Ident uint64

var ZeroRange Ident

func (id Ident) String() string {
	return fmt.Sprintf("%d", id)
}
