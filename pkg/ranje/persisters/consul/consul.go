package consul

import (
	"fmt"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/hashicorp/consul/api"
)

type Persister struct {
	kv *api.KV
}

func New(client *api.Client) *Persister {
	return &Persister{
		kv: client.KV(),
	}
}

func (cp *Persister) Get(m ranje.Meta) (*ranje.Range, error) {
	return nil, fmt.Errorf("not implemented")
}

func (cp *Persister) PutState(r *ranje.Range, new ranje.StateLocal) error {

	// TODO: Do something with the Scope here
	k := fmt.Sprintf("ranges/%d/state", r.Meta.Ident.Key)

	v, _, err := cp.kv.Get(k, nil)
	if err != nil {
		return err
	}

	old := string(v.Value)
	if r.State().String() != old {
		return fmt.Errorf(
			"expected range %d to be in state %v, got %v",
			r.Meta.Ident.Key, r.State().String(), old)
	}

	// TODO: Use Acquire to lock the key between get/put

	p := &api.KVPair{
		Key:   k,
		Value: []byte(new.String()),
	}

	_, err = cp.kv.Put(p, nil)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Should maybe use check-and-set here rather than put, to avoid races.
func (cp *Persister) Create(r *ranje.Range) error {

	// TODO: Do something with the Scope here
	pfx := fmt.Sprintf("ranges/%d", r.Meta.Ident.Key)

	ops := api.KVTxnOps{
		&api.KVTxnOp{
			Verb:  api.KVSet,
			Key:   fmt.Sprintf("%s/start", pfx),
			Value: []byte(r.Meta.Start),
		},
		&api.KVTxnOp{
			Verb:  api.KVSet,
			Key:   fmt.Sprintf("%s/end", pfx),
			Value: []byte(r.Meta.End),
		},
		&api.KVTxnOp{
			Verb:  api.KVSet,
			Key:   fmt.Sprintf("%s/state", pfx),
			Value: []byte(r.State().String()),
		},
	}

	for _, id := range r.ParentIdents() {
		ops = append(ops, &api.KVTxnOp{
			Verb:  api.KVSet,
			Key:   fmt.Sprintf("%s/parents/%d", pfx, id.Key),
			Value: nil,
		})
	}

	ok, _, _, err := cp.kv.Txn(ops, nil)
	if err != nil {
		return err
	}
	if !ok {
		// This should not happen
		return fmt.Errorf("got no err but !ok from kv.Txn?")
	}

	return nil
}
