package ranje

// Key is a point in the keyspace.
type Key string

// Special case representing both negative and positive infinity.
// Don't compare anything against this! Always check for it explicitly.
const ZeroKey Key = ""
