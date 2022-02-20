package consul

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/adammck/ranger/pkg/ranje"
	"github.com/hashicorp/consul/api"
)

type Persister struct {
	kv *api.KV

	// keep track of the last ModifyIndex for each range.
	// Note that the key is a *pointer* which is weird.
	modifyIndex map[*ranje.Range]uint64

	// guards modifyIndex
	sync.Mutex
}

func New(client *api.Client) *Persister {
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

		if key != r.Meta.Ident.Key {
			log.Printf("mismatch between Consul KV key and encoded range: key=%v, r.meta.ident.key=%v", key, r.Meta.Ident.Key)
			continue
		}

		// Update
		cp.modifyIndex[r] = kv.ModifyIndex

		out = append(out, r)
	}

	return out, nil
}

func (cp *Persister) Put(r *ranje.Range) error {
	v, err := json.Marshal(r)
	if err != nil {
		return err
	}

	// TODO: Lock each range rather than the whole map! The lock is held for the whole RPC.
	cp.Lock()
	defer cp.Unlock()

	op := &api.KVTxnOp{
		Verb:  api.KVCAS,
		Key:   fmt.Sprintf("ranges/%d", r.Meta.Ident.Key),
		Value: v,
	}

	if index, ok := cp.modifyIndex[r]; ok {
		op.Index = index
	}

	ok, res, _, err := cp.kv.Txn(api.KVTxnOps{op}, nil)
	if err != nil {
		return err
	}
	if !ok {
		// This should never happen
		panic("got no err but !ok from Txn?")
	}
	if len(res.Results) != 1 {
		panic(fmt.Sprintf("expected one result from Txn, got %d", len(res.Results)))
	}

	cp.modifyIndex[r] = res.Results[0].ModifyIndex

	return nil
}
