package roster2

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

type Roster2 struct {
	// ident -> ranges
	// that's Node ident, not Range Ident!
	Map map[string]*ShortNode
	sync.RWMutex

	disc discovery.Discoverable

	// Callbacks
	add    func(rem *discovery.Remote)
	remove func(rem *discovery.Remote)
}

const (
	probeTimeout = 1 * time.Second
)

func New(disc discovery.Discoverable, add, remove func(rem *discovery.Remote)) *Roster2 {
	return &Roster2{
		Map:    make(map[string]*ShortNode),
		disc:   disc,
		add:    add,
		remove: remove,
	}
}

// TODO: Replace this with a statusz-type page
func (ros *Roster2) DumpForDebug() {
	ros.RLock()
	defer ros.RUnlock()

	// Sorted list of keys for stable output.
	keys := make([]string, 0, len(ros.Map))
	for k := range ros.Map {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, nid := range keys {
		fmt.Printf(" - %s\n", nid)

		for m, r := range ros.Map[nid].ranges {
			fmt.Printf("    - %s: %s\n", m.String(), r.String())
		}
	}
}

// Locate returns the list of node IDs that the given key can be found on, in any state.
// TODO: Allow the Map to be filtered by state.
func (ros *Roster2) Locate(k ranje.Key) []string {
	nodes := []string{}

	ros.RLock()
	defer ros.RUnlock()

	// look i'm in a hurry here okay
	for nid, n := range ros.Map {
		func() {
			n.muRanges.RLock()
			defer n.muRanges.RUnlock()

			for m := range n.ranges {
				if m.Contains(k) {
					nodes = append(nodes, nid)
				}
			}
		}()
	}

	return nodes
}

// Caller must hold ros.RWMutex
func (ros *Roster2) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, rem := range res {
		n, ok := ros.Map[rem.Ident]

		// New Node?
		if !ok {
			n = NewShortNode(rem)
			fmt.Printf("new node: %v -> %s\n", rem.Ident, rem.Addr())
			ros.Map[rem.Ident] = n

			// TODO: Do this outside of the lock!!
			if ros.add != nil {
				ros.add(&n.remote)
			}
		}

		n.Seen(time.Now())
	}
}

// Caller must hold ros.RWMutex
func (ros *Roster2) expire() {
	now := time.Now()

	for nid, n := range ros.Map {
		if n.IsStale(now) {
			delete(ros.Map, nid)
			fmt.Printf("node expired: %v\n", nid)

			// TODO: Do this outside of the lock!!
			if ros.remove != nil {
				ros.remove(&n.remote)
			}
		}
	}
}

// probeOne sends an RPC to fetch the current ranges for one node.
// Returns error if the RPC fails or if a probe is already in progess.
func probeOne(ctx context.Context, n *ShortNode) error {
	ranges := make(map[ranje.Meta]ranje.StateRemote)

	res, err := n.client.Ranges(ctx, &pb.RangesRequest{})
	if err != nil {
		fmt.Printf("Probe failed: %s\n", err)
		return err
	}

	for _, r := range res.Ranges {
		if r.Meta == nil {
			fmt.Printf("Malformed probe response from node %s: Meta is nil\n", n.remote.Ident)
			continue
		}

		m, err := ranje.MetaFromProto(r.Meta)
		if r.Meta == nil {
			fmt.Printf("Malformed probe response from node %s: %s\n", n.remote.Ident, err)
			continue
		}

		// TODO: Update the map rather than overwriting it every time.
		ranges[*m] = ranje.RemoteStateFromProto(r.State)
	}

	// TODO: Do we need a range-changed callback?

	n.muRanges.Lock()
	n.ranges = ranges
	n.muRanges.Unlock()

	return nil
}

// TODO: Replace this polling with streaming RPCs.
// Caller must hold ros.RWMutex
func (ros *Roster2) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	// Measure how long this takes and how many succeed.
	start := time.Now()
	var success uint64

	for _, node := range ros.Map {
		wg.Add(1)

		// Copy node since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *ShortNode) {
			defer wg.Done()
			err := probeOne(ctx, n)
			if err != nil {
				fmt.Printf("probe error: %s\n", err)
				return
			}
			atomic.AddUint64(&success, 1)
		}(node)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Printf("probed %d nodes in %s\n", success, elapsed.String())
}

func (r *Roster2) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.discover()

	// Expire any nodes that have gone missing service discovery.
	r.expire()

	r.probe()
}

func (r *Roster2) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}
