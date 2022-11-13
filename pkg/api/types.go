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
