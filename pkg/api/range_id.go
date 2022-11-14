package api

import "fmt"

// RangeID is the unique identity of a range.
type RangeID uint64

// ZeroRange is not a valid RangeID.
const ZeroRange RangeID = 0

func (id RangeID) String() string {
	return fmt.Sprintf("%d", id)
}
