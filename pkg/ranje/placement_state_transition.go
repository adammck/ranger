package ranje

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
)

type PlacementStateTransition struct {
	from api.PlacementState
	to   api.PlacementState
}

var PlacementStateTransitions []PlacementStateTransition

func init() {
	PlacementStateTransitions = []PlacementStateTransition{
		// Happy Path
		{api.PsPending, api.PsInactive}, // Prepare
		{api.PsInactive, api.PsActive},  // Serve
		{api.PsActive, api.PsInactive},  // Take
		{api.PsInactive, api.PsDropped}, // Drop

		// Node crashed (or placement mysteriously vanished)
		{api.PsPending, api.PsGiveUp},
		{api.PsInactive, api.PsGiveUp},
		{api.PsActive, api.PsGiveUp},

		// Recovery?
		{api.PsGiveUp, api.PsDropped},
	}
}

func CanTransitionPlacement(from, to api.PlacementState) error {
	for _, t := range PlacementStateTransitions {
		if t.from == from && t.to == to {
			return nil
		}
	}

	return fmt.Errorf("invalid transition: from=%s, to:%s", from.String(), to.String())
}
