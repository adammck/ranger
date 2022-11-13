package api

type RemoteState uint8

const (

	// Should never be in this state. Indicates a bug.
	NsUnknown RemoteState = iota

	// Stable states
	NsInactive
	NsActive

	// During transitions
	NsLoading      // Pending  -> PreReady
	NsActivating   // PreReady -> Ready
	NsDeactivating // Ready    -> PreReady
	NsDropping     // PreReady -> NotFound

	// Special case: This is never returned by probes, since those only include
	// the state of ranges which the node has. This is returned by redundant
	// Drop RPCs which instruct nodes to drop a range that they don't have.
	// (Maybe it was already dropped, or maybe the node never had it. Can't
	// know.) This is a success, not an error, because those RPCs may be
	// received multiple times during a normal drop, and should be treated
	// idempotently. But we don't want to return NsUnknown, because we do know.
	NsNotFound
)

//go:generate stringer -type=RemoteState -output=zzz_remote_state.go
