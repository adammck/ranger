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
		{api.PsInactive, api.PsActive},  // Activate
		{api.PsActive, api.PsInactive},  // Deactivate
		{api.PsInactive, api.PsDropped}, // Drop

		// Node crashed (or placement mysteriously vanished)
		{api.PsPending, api.PsMissing},
		{api.PsInactive, api.PsMissing},
		{api.PsActive, api.PsMissing},

		// Recovery?
		{api.PsMissing, api.PsDropped},
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
