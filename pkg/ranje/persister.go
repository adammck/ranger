package ranje

type Persister interface {
	GetRanges() ([]*Range, error)
	Put(*Range) error
}
