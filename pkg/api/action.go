package api

// Action represents each of the state transitions that ranger can ask a node
// to make. (They're named for the RPC interface, but it's pluggable.) They're
// exposed here for testing. See also Command.
type Action uint8

const (
	Give Action = iota
	Serve
	Take
	Drop
)

//go:generate stringer -type=Action -output=zzz_action_string.go
