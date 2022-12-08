package keyspace

import (
	"log"
	"sort"

	"github.com/adammck/ranger/pkg/api"
)

type Repl struct {
	Start  api.Key
	End    api.Key
	Total  int
	Active int
}

func (ks *Keyspace) ReplicationState() []Repl {
	flat := flatRanges(ks)

	for i := range flat {
		for _, r := range ks.ranges {
			if r.State == api.RsObsolete {
				continue
			}
			if r.Meta.Contains(flat[i].Start) {
				log.Printf("%s: r=%s, t=%d, a=%d", flat[i].Start, r.String(), len(r.Placements), r.NumPlacementsInState(api.PsActive))
				flat[i].Total += len(r.Placements)
				flat[i].Active += r.NumPlacementsInState(api.PsActive)
			}
		}
	}

	return flat
}

func flatRanges(ks *Keyspace) []Repl {

	keyMap := make(map[api.Key]struct{}, len(ks.ranges))
	for _, r := range ks.ranges {
		if r.State == api.RsObsolete {
			// Obsolete ranges can't have placements, so skip them.
			continue
		}

		if r.Meta.Start != api.ZeroKey {
			keyMap[r.Meta.Start] = struct{}{}
		}

		if r.Meta.End != api.ZeroKey {
			keyMap[r.Meta.End] = struct{}{}
		}
	}

	i := 0
	keyList := make([]api.Key, len(keyMap))
	for k := range keyMap {
		keyList[i] = k
		i++
	}

	sort.Slice(keyList, func(i, j int) bool {
		return keyList[i] < keyList[j]
	})

	if len(keyMap) == 0 {
		return []Repl{
			{
				Start: api.ZeroKey,
				End:   api.ZeroKey,
			},
		}
	}

	out := make([]Repl, len(keyList)+1)
	for i := 0; i < len(keyList); i++ {
		out[i].End = keyList[i]

		if i < len(keyList) {
			out[i+1].Start = keyList[i]
		}
	}

	return out
}
