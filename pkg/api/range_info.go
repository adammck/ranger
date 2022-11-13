package api

// RangeInfo represents something we know about a Range on a Node at a moment in
// time. These are emitted and cached by the Roster to anyone who cares.
type RangeInfo struct {
	Meta  Meta
	State RemoteState
	Info  LoadInfo
}
