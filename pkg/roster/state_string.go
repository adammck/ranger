// Code generated by "stringer -type=State -output=state_string.go"; DO NOT EDIT.

package roster

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[StateUnknown-0]
	_ = x[StateFetching-1]
	_ = x[StateFetched-2]
	_ = x[StateFetchFailed-3]
	_ = x[StateReady-4]
	_ = x[StateTaken-5]
}

const _State_name = "StateUnknownStateFetchingStateFetchedStateFetchFailedStateReadyStateTaken"

var _State_index = [...]uint8{0, 12, 25, 37, 53, 63, 73}

func (i State) String() string {
	if i >= State(len(_State_index)-1) {
		return "State(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _State_name[_State_index[i]:_State_index[i+1]]
}