package api

import "time"

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  Meta
	State RemoteState
	Info  LoadInfo

	// Expire contains the time at which the "activation lease" should expire,
	// i.e. the Rangelet should unilaterally (without input from the controller)
	// deactivate the range. This is only non-zero when State is NsActive.
	Expire time.Time
}
