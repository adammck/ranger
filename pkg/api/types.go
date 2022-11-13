package api

type Placement struct {
	Node  string
	State PlacementState
}

type Parent struct {
	Meta       Meta
	Parents    []Ident
	Placements []Placement
}

// Same as roster/info.LoadInfo, to avoid circular import.
type LoadInfo struct {
	Keys   int
	Splits []Key
}
