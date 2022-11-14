package api

import "fmt"

// RangeID is the unique identity of a range.
type RangeID uint64

// ZeroRange is not a valid RangeID.
var ZeroRange RangeID

func (id RangeID) String() string {
	return fmt.Sprintf("%d", id)
}
