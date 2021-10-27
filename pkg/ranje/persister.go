package ranje

type Persister interface {
	Get(m Meta) (*Range, error)
	Create(r *Range) error
	PutState(r *Range, new StateLocal) error
}
