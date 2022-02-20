package roster

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

const (
	probeTimeout = 3 * time.Second
)

type Roster struct {
	// Public so Balancer can read the Nodes
	// node ident (not ranje.Ident!!) -> Node
	Nodes map[string]*Node
	sync.RWMutex

	disc discovery.Discoverable

	// Callbacks
	add    func(rem *discovery.Remote)
	remove func(rem *discovery.Remote)
}

func New(disc discovery.Discoverable, add, remove func(rem *discovery.Remote)) *Roster {
	return &Roster{
		Nodes:  make(map[string]*Node),
		disc:   disc,
		add:    add,
		remove: remove,
	}
}

//func (ros *Roster) NodeBy(opts... NodeByOpts)
// TODO: Return an error from this func, to avoid duplicating it in callers.
func (ros *Roster) NodeByIdent(nodeIdent string) *Node {
	ros.RLock()
	defer ros.RUnlock()

	for nid, n := range ros.Nodes {
		if nid == nodeIdent {
			return n
		}
	}

	return nil
}

// Locate returns the list of node IDs that the given key can be found on, in any state.
// TODO: Allow the Map to be filtered by state.
func (ros *Roster) Locate(k ranje.Key) []string {
	nodes := []string{}

	ros.RLock()
	defer ros.RUnlock()

	// look i'm in a hurry here okay
	for nid, n := range ros.Nodes {
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
func (ros *Roster) discover() {
	res, err := ros.disc.Get("node")
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		n, ok := ros.Nodes[r.Ident]

		// New Node?
		if !ok {
			n = NewNode(r)
			ros.Nodes[r.Ident] = n
			log.Printf("added node: %v", n.Ident())

			// TODO: Do this outside of the lock!!
			if ros.add != nil {
				ros.add(&n.Remote)
			}
		}

		n.Seen(time.Now())
	}
}

// Caller must hold ros.RWMutex
func (ros *Roster) expire() {
	now := time.Now()

	for nID, n := range ros.Nodes {
		if n.IsStale(now) {
			// TODO: Don't do this! Mark it as expired instead. There might still be ranges placed on it which need cleaning up.
			delete(ros.Nodes, nID)
			log.Printf("expired node: %v", n.Ident())

			// TODO: Do this outside of the lock!!
			if ros.remove != nil {
				ros.remove(&n.Remote)
			}
		}
	}
}

func (ros *Roster) probe() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	// TODO: We already do this in expire, bundle them together.
	for _, node := range ros.Nodes {
		wg.Add(1)

		// Copy node since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *Node) {
			defer wg.Done()
			err := probeOne(ctx, n)
			if err != nil {
				log.Printf("error probing %v: %v", n.Ident(), err)
				return
			}
		}(node)
	}

	wg.Wait()
}

// probeOne sends an RPC to fetch the current ranges for one node.
// Returns error if the RPC fails or if a probe is already in progess.
func probeOne(ctx context.Context, n *Node) error {
	// TODO: Abort if probe in progress.

	ranges := make(map[ranje.Meta]State)

	// TODO: Merge Info and Ranges RPCs. Unclear which was wich.
	// 	res, err := n.Client.Info(ctx, &pb.InfoRequest{})

	res, err := n.Client.Ranges(ctx, &pb.RangesRequest{})
	if err != nil {
		log.Printf("probe failed: %s", err)
		return err
	}

	for _, r := range res.Ranges {
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: Meta is nil", n.Remote.Ident)
			continue
		}

		m, err := ranje.MetaFromProto(r.Meta)
		if r.Meta == nil {
			log.Printf("malformed probe response from %v: %v", n.Remote.Ident, err)
			continue
		}

		// TODO: Update the map rather than overwriting it every time.
		ranges[*m] = RemoteStateFromProto(r.State)
	}

	// TODO: Do we need a range-changed callback?

	n.muRanges.Lock()
	n.ranges = ranges
	n.muRanges.Unlock()

	return nil
}

// 	for _, r := range res.Ranges {
// 		rr := r.Range
// 		if rr == nil {
// 			log.Printf("Malformed probe response from node %s: Range is nil", n.Remote.Ident)
// 			continue
// 		}

// 		// id, err := IdentFromProto(rr.Ident)
// 		// if err != nil {
// 		// 	log.Printf("Got malformed ident from node %s: %s", n.addr(), err.Error())
// 		// 	continue
// 		// }

// 		// TODO: Move all of this outwards to some node state coordinator

// 		// p, ok := n.ranges[*id]

// 		// if !ok {
// 		// 	log.Printf("Got unexpected range from node %s: %s", n.addr(), id.String())
// 		// 	n.unexpectedRanges[*id] = rr
// 		// 	continue
// 		// }

// 		// updateLocalState(p, r.State)

// 		// TODO: We compare the Ident here even though we just fetched the assignment by ID. Is that... why
// 		// if !p.rang.SameMeta(*id, rr.Start, rr.End) {
// 		// 	log.Printf("Remote range did not match local range with same ident: %s", id.String())
// 		// 	continue
// 		// }

// 		// Finally update the remote info
// 		//p.K = r.Keys

// 		// TODO: Figure out wtf to do when remote state doesn't match local
// 		//rrr.state = RemoteStateFromProto(r.State)
// 	}

func (r *Roster) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.discover()

	// Expire any nodes that have gone missing service discovery.
	r.expire()

	r.probe()
}

func (r *Roster) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}
