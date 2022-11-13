package ranje

import (
	"github.com/adammck/ranger/pkg/api"
)

type RangeStateTransition struct {
	from api.RangeState
	to   api.RangeState
}

var RangeStateTransitions []RangeStateTransition

func init() {
	RangeStateTransitions = []RangeStateTransition{
		{api.RsActive, api.RsSubsuming},
		{api.RsSubsuming, api.RsObsolete},
		{api.RsSubsuming, api.RsObsolete},

		{api.RsActive, api.RsSubsuming},
		{api.RsSubsuming, api.RsObsolete},
		{api.RsSubsuming, api.RsObsolete},
	}
}
