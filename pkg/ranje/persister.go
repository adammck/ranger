package ranje

type Persister interface {
	GetRanges() ([]*Range, error)
	PutRange(*Range) error
}
