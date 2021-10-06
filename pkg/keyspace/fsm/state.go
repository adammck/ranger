package fsm

type State uint8

// TODO: Is joining and splitting the same state?

const (
	Pending State = iota
	Ready
	Splitting
	Joining
	Obsolete
	// Discarding
)

//go:generate stringer -type=State
