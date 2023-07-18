package roster

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	probeTimeout = 3 * time.Second
)

type Roster struct {

	// How long should the controller wait for a node to respond to a probe
	// before expiring it? The default value (zero) means that nodes expire as
	// soon as they are discovered, which means that nothing every works. That
	// is non-ideal.
	NodeExpireDuration time.Duration

	// TODO: Can this be private now?
	Nodes map[api.NodeID]*Node
	sync.RWMutex

	disc discovery.Getter

	// Callbacks
	add    func(rem *api.Remote)
	remove func(rem *api.Remote)

	// info receives NodeInfo updated whenever we receive a probe response from
	// a node, or when we expire a node.
	info chan NodeInfo

	// To be stubbed when testing.
	NodeConnFactory func(ctx context.Context, remote api.Remote) (*grpc.ClientConn, error)
}

func New(disc discovery.Discoverer, add, remove func(rem *api.Remote), info chan NodeInfo) *Roster {
	return &Roster{
		Nodes:  make(map[api.NodeID]*Node),
		disc:   disc.Discover("node", nil, nil),
		add:    add,
		remove: remove,
		info:   info, // currently never closed

		// Defaults to production implementation.
		// Patch it after construction for tests.
		NodeConnFactory: nodeConnFactory,

		NodeExpireDuration: 1 * time.Minute,
	}
}

// TODO: This is only used by tests. Maybe move it there?
func (ros *Roster) TestString() string {
	ros.RLock()
	defer ros.RUnlock()

	nIDs := []api.NodeID{}
	for nID := range ros.Nodes {
		nIDs = append(nIDs, nID)
	}

	sort.Slice(nIDs, func(i, j int) bool {
		return nIDs[i].String() < nIDs[j].String()
	})

	s := make([]string, len(ros.Nodes))
	for i, nID := range nIDs {
		s[i] = ros.Nodes[nID].TestString()
	}

	return strings.Join(s, " ")
}

// nodeFactory returns a new node connected via a real gRPC connection.
func nodeConnFactory(ctx context.Context, remote api.Remote) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, remote.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (ros *Roster) NodeByIdent(nID api.NodeID) (*Node, error) {
	ros.RLock()
	defer ros.RUnlock()

	for nid, n := range ros.Nodes {
		if nid == nID {
			return n, nil
		}
	}

	return nil, ErrNodeNotFound{nID}
}

// Location is returned by the Locate method. Don't use it for anything else.
type Location struct {
	Node api.NodeID
	Info api.RangeInfo
}

// Locate returns the list of node IDs that the given key can be found on, and
// the state of the range containing the key.
func (ros *Roster) Locate(k api.Key) []Location {
	return ros.LocateInState(k, []api.RemoteState{})
}

func (ros *Roster) LocateInState(k api.Key, states []api.RemoteState) []Location {
	nodes := []Location{}

	ros.RLock()
	defer ros.RUnlock()

	// look i'm in a hurry here okay
	for _, n := range ros.Nodes {
		func() {
			n.muRanges.RLock()
			defer n.muRanges.RUnlock()

			for _, ri := range n.ranges {
				if ri.Meta.Contains(k) {

					// Skip if not in one of given states.
					if len(states) > 0 {
						ok := false
						for _, s := range states {
							if ri.State == s {
								ok = true
								break
							}
						}
						if !ok {
							continue
						}
					}

					nodes = append(nodes, Location{
						Node: n.Ident(),
						Info: *ri,
					})
				}
			}
		}()
	}

	return nodes
}

// Caller must hold ros.RWMutex
func (ros *Roster) Discover() {
	res, err := ros.disc.Get()
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		n, ok := ros.Nodes[r.NodeID()]

		// New Node?
		if !ok {
			// TODO: Propagate context from somewhere.
			conn, err := ros.NodeConnFactory(context.Background(), r)
			if err != nil {
				continue
			}

			n = NewNode(r, conn)
			ros.Nodes[r.NodeID()] = n

			// TODO: Should we also send a blank NodeInfo to introduce the node?
			// We haven't probed yet, so don't know what's assigned.

			// TODO: Do this outside of the lock!!
			if ros.add != nil {
				ros.add(&n.Remote)
			}
		}

		n.whenLastSeen = time.Now()
	}
}

// Caller must hold ros.RWMutex
func (ros *Roster) expire() {
	now := time.Now()

	for nID, n := range ros.Nodes {
		if n.IsMissing(ros.NodeExpireDuration, now) {
			// The node hasn't responded to probes in a while. Send a special
			// loadinfo to the reconciler, to tell it that we've lost the node.
			if ros.info != nil {
				ros.info <- NodeInfo{
					Time:    time.Now(),
					NodeID:  n.Ident(),
					Expired: true,
				}
			}

			// TODO: Get rid of these callbacks. Just use the channel.
			if ros.remove != nil {
				ros.remove(&n.Remote)
			}

			if n.IsGoneFromServiceDiscovery(now) {
				// The node has also been missing from service discovery for a
				// while, so we can forget about it and stop probing.
				delete(ros.Nodes, nID)
				continue
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

		// Copy node pointer since it changes between iterations.
		// https://golang.org/doc/faq#closures_and_goroutines
		go func(n *Node) {
			defer wg.Done()
			err := ros.probeOne(ctx, n)
			if err != nil {
				return
			}
		}(node)
	}

	wg.Wait()
}

// probeOne sends an RPC to fetch the current ranges for one node.
// Returns error if the RPC fails or if a probe is already in progess.
func (ros *Roster) probeOne(ctx context.Context, n *Node) error {
	// TODO: Abort if probe in progress.

	ranges := make(map[api.RangeID]*api.RangeInfo)

	// TODO: This is an InfoRequest now, but we also have RangesRequest which is
	// sufficient for the proxy. Maybe make which one is sent configurable?

	// TODO: Move this into Node, so the Client can be private.

	res, err := n.Client.Info(ctx, &pb.InfoRequest{})
	if err != nil {
		return err
	}

	ni := NodeInfo{
		Time:   time.Now(),
		NodeID: n.Ident(),
	}

	for _, r := range res.Ranges {

		ri, err := conv.RangeInfoFromProto(r)
		if err != nil {
			// TODO: Do something other than log this?
			log.Printf("malformed probe response from %v: %v", n.Remote.Ident, err)
			continue
		}

		ni.Ranges = append(ni.Ranges, ri)
		ranges[ri.Meta.Ident] = &ri
	}

	// TODO: Should this (nil info) even be allowed?
	if ros.info != nil {
		ros.info <- ni
	}

	// TODO: Do we need a range-changed callback?

	n.muRanges.Lock()
	n.wantDrain = res.WantDrain
	n.ranges = ranges
	n.muRanges.Unlock()

	n.whenLastProbed = time.Now()

	return nil
}

func (r *Roster) Tick() {
	// TODO: anything but this
	r.Lock()
	defer r.Unlock()

	// Grab any new nodes from service discovery.
	r.Discover()

	r.probe()

	// Expire any nodes that we haven't been able to probe in a while.
	r.expire()
}

// TODO: Need some way to gracefully stop! Have to close the info channel to
// stop the reconciler.
func (r *Roster) Run(t *time.Ticker) {
	for ; true; <-t.C {
		r.Tick()
	}
}

// Candidate returns the NodeIdent of a node which could accept the given range.
//
// TODO: Instead of an actual range, this should take a "pseudo-range" which is
// either a single range (wanting to split) and split point, or two ranges
// (wanting to join). Or I guess just a wrapper around a single (moving) range.
// From these, we can estimate how much capacity the candidate node(s) will
// need, allowing us to find candidates before actually performing splits and
// joins.
func (r *Roster) Candidate(rng *ranje.Range, c ranje.Constraint) (api.NodeID, error) {
	r.RLock()
	defer r.RUnlock()

	// Build a list of nodes.
	// TODO: Just store them this way!

	nodes := make([]*Node, len(r.Nodes))
	i := 0

	for _, nod := range r.Nodes {
		nodes[i] = nod
		i += 1
	}

	// Build a list of indices of candidate nodes.
	candidates := []int{}

	// Filter the list of nodes by name
	// (We still might exclude it below though)
	if c.NodeID != "" {
		found := false

		for i := range nodes {
			if nodes[i].Ident() == c.NodeID {
				nodes = []*Node{nodes[i]}
				found = true
				break
			}
		}

		// Specific NodeID was given, but no such node was found.
		if !found {
			return "", fmt.Errorf("no such node: %v", c.NodeID)
		}
	}

	// Convert slice of excluded nodes into a map for easy member check.
	excluded := map[api.NodeID]struct{}{}
	for _, nID := range c.Not {
		excluded[nID] = struct{}{}
	}

	rID := api.ZeroRange
	if rng != nil {
		rID = rng.Meta.Ident
	}

	// Exclude a node if:
	//
	// 1. It has been explicitly excluded.
	//
	// 2. It already has this range.
	//
	// 3. It's drained, i.e. it doesn't want any more ranges. It's probably
	//    shutting down.
	//
	// 4. It's missing, i.e. hasn't responded to our probes in a while. It might
	//    still come back, but let's avoid it anyway.
	//
	// 5. This range has failed to place on this node within the past minute.
	//
	for i := range nodes {
		if _, ok := excluded[nodes[i].Ident()]; ok {
			if c.NodeID != "" {
				return "", fmt.Errorf("node is excluded: %v", nodes[i].Ident())
			}

			continue
		}

		if rID != api.ZeroRange && nodes[i].HasRange(rng.Meta.Ident) {
			if c.NodeID != "" {
				return "", fmt.Errorf("node already has range: %v", c.NodeID)
			}

			continue
		}

		if nodes[i].WantDrain() {
			if c.NodeID != "" {
				return "", fmt.Errorf("node wants drain: %v", nodes[i].Ident())
			}

			continue
		}

		if nodes[i].IsMissing(r.NodeExpireDuration, time.Now()) {
			if c.NodeID != "" {
				return "", fmt.Errorf("node is missing: %v", nodes[i].Ident())
			}

			continue
		}

		// node has recent failed placements
		if nodes[i].PlacementFailures(rID, time.Now().Add(-1*time.Minute)) >= 1 {
			if c.NodeID == "" {
				continue
			}
		}

		candidates = append(candidates, i)
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no candidates available (rID=%v, c=%v)", rID, c)
	}

	// Pick the node with lowest utilization. For nodes with the exact same
	// utilization, pick the node with the lowest (lexicographically) ident.
	//
	// TODO: This doesn't take into account ranges which are on the way to that
	// node, and is generally totally insufficient.

	sort.Slice(candidates, func(i, j int) bool {
		ci := nodes[candidates[i]]
		cj := nodes[candidates[j]]

		ciu := ci.Utilization()
		cju := cj.Utilization()
		if ciu != cju {
			return ciu < cju
		}

		return ci.Ident() < cj.Ident()
	})

	return nodes[candidates[0]].Ident(), nil
}
