// Code generated by "stringer -type=RangeState -output=range_state_string.go"; DO NOT EDIT.

package ranje

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[RsUnknown-0]
	_ = x[RsActive-1]
	_ = x[RsSubsuming-2]
	_ = x[RsObsolete-3]
}

const _RangeState_name = "RsUnknownRsActiveRsSubsumingRsObsolete"

var _RangeState_index = [...]uint8{0, 9, 17, 28, 38}

func (i RangeState) String() string {
	if i >= RangeState(len(_RangeState_index)-1) {
		return "RangeState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _RangeState_name[_RangeState_index[i]:_RangeState_index[i+1]]
}
