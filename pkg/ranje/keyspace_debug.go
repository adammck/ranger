package ranje

// KeyspaceDebug is a wrapper around Keyspace which allows access to internal
// state which should not be touched. It's here so callers have to be really
// clear that this is what they want.
//
// Instantiate it via Keyspace.DangerousDebuggingMethods
type keyspaceDebug struct {
	*Keyspace
}

// Ranges returns all of the ranges which aren't Obsolete in no particular
// order.
func (ks *keyspaceDebug) Ranges() []*Range {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	return ks.ranges
}
