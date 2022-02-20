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
func (ks *keyspaceDebug) NonObsoleteRanges() []*Range {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	out := []*Range{}

	for _, r := range ks.ranges {
		if r.State == Obsolete {
			continue
		}

		out = append(out, r)
	}

	return out
}
