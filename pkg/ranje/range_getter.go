package ranje

import "github.com/adammck/ranger/pkg/api"

// RangeGetter allows callers to fetch a Range from its RangeIdent.
type RangeGetter interface {
	Get(id api.Ident) (*Range, error)
}
