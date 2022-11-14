package consul

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	rapi "github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	capi "github.com/hashicorp/consul/api"
)

type Persister struct {
	kv *capi.KV

	// keep track of the last ModifyIndex for each range.
	// Note that the key is a *pointer* which is weird.
	modifyIndex map[*ranje.Range]uint64

	// guards modifyIndex
	sync.Mutex
}

func New(client *capi.Client) *Persister {
	return &Persister{
		kv:          client.KV(),
		modifyIndex: map[*ranje.Range]uint64{},
	}
}

func (cp *Persister) GetRanges() ([]*ranje.Range, error) {
	pairs, _, err := cp.kv.List("/ranges", nil)
	if err != nil {
		return nil, err
	}

	out := []*ranje.Range{}

	// TODO: Something less dumb than this.
	cp.Lock()
	defer cp.Unlock()

	for _, kv := range pairs {
		s := strings.SplitN(kv.Key, "/", 2)
		if len(s) != 2 {
			log.Printf("WARN: invalid Consul key: %s", kv.Key)
			continue
		}

		key, err := strconv.ParseUint(s[1], 10, 64)
		if err != nil {
			log.Printf("WARN: invalid Consul key: %s", kv.Key)
			continue
		}

		r := &ranje.Range{}
		json.Unmarshal(kv.Value, r)

		rID := rapi.RangeID(key)
		if rID != r.Meta.Ident {
			log.Printf("mismatch between Consul KV key and encoded range: key=%v, r.meta.ident=%v", key, r.Meta.Ident)
			continue
		}

		// Update
		cp.modifyIndex[r] = kv.ModifyIndex

		out = append(out, r)
	}

	return out, nil
}

func (cp *Persister) PutRanges(ranges []*ranje.Range) error {
	cp.Lock()
	defer cp.Unlock()

	var ops capi.KVTxnOps
	keyToRange := map[string]*ranje.Range{}

	for _, r := range ranges {
		v, err := json.Marshal(r)
		if err != nil {
			return err
		}

		op := &capi.KVTxnOp{
			Verb:  capi.KVCAS,
			Key:   fmt.Sprintf("ranges/%d", r.Meta.Ident),
			Value: v,
		}

		// Keep track of which range each key came from, so we can update the
		// modifyIndex cache when we receive the response.
		// TODO: Maybe use the op.Key as the map key here instead?
		keyToRange[op.Key] = r

		if index, ok := cp.modifyIndex[r]; ok {
			op.Index = index
		}

		ops = append(ops, op)
	}

	ok, res, _, err := cp.kv.Txn(ops, nil)
	if err != nil {
		return err
	}
	if !ok {
		// This should never happen
		panic("got no err but !ok from Txn?")
	}
	if len(res.Results) != len(ops) {
		panic(fmt.Sprintf("expected %d result from Txn, got %d", len(ops), len(res.Results)))
	}

	for _, res := range res.Results {
		r := keyToRange[res.Key]
		cp.modifyIndex[r] = res.ModifyIndex
	}

	return nil
}
