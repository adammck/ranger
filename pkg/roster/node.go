package roster

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

type PlacementFailure struct {
	rID  api.RangeID
	when time.Time
}

type Node struct {
	Remote api.Remote

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// When this node was last seen in service discovery. Doesn't necessarily
	// mean that it's actually alive, though.
	whenLastSeen time.Time

	// When this node was last successfully whenLastProbed. This means that it's
	// actually up and healthy enough to respond.
	whenLastProbed time.Time

	// Keep track of when placements are attempted but fail, so that we can try
	// placement on a different node rather than the same one again. Note that
	// this is (currently) volatile, and is forgotten when the controller restarts.
	placementFailures []PlacementFailure
	muPF              sync.RWMutex

	// The gRPC connection to the actual remote node.
	conn   *grpc.ClientConn
	Client pb.NodeClient

	// Populated by probeOne
	wantDrain bool
	ranges    map[api.RangeID]*api.RangeInfo
	muRanges  sync.RWMutex
}

func NewNode(remote api.Remote, conn *grpc.ClientConn) *Node {
	return &Node{
		Remote:            remote,
		init:              time.Now(),
		whenLastSeen:      time.Time{}, // never
		whenLastProbed:    time.Time{}, // never
		placementFailures: []PlacementFailure{},
		conn:              conn,
		Client:            pb.NewNodeClient(conn),
		ranges:            make(map[api.RangeID]*api.RangeInfo),
	}
}

func (n *Node) UpdateRangeInfo(ri *api.RangeInfo) {
	n.muRanges.Lock()
	defer n.muRanges.Unlock()
	n.ranges[ri.Meta.Ident] = ri
}

func (n *Node) UpdateRangeState(rID api.RangeID, s api.RemoteState) error {
	n.muRanges.Lock()
	defer n.muRanges.Unlock()

	// Forget the range. If the range is not in the map, that's probably a race
	// condition between an RPC and a probe. That shouldn't happen, but is a
	// no-op anyway, so let's ignore it.
	if s == api.NsNotFound {
		delete(n.ranges, rID)
		return nil
	}

	ri, ok := n.ranges[rID]
	if ok {
		ri.State = s
		return nil
	}

	return fmt.Errorf(
		"missing from range cache: nID=%v, rID=%v",
		n.Ident(), rID)
}

// TODO: This is only used by tests. Maybe move it there?
func (n *Node) TestString() string {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()

	rIDs := []api.RangeID{}
	for rID := range n.ranges {
		rIDs = append(rIDs, rID)
	}

	// Sort by (numeric) range ID to make output stable.
	// (Unstable sort is fine, because range IDs are unique.)
	sort.Slice(rIDs, func(i, j int) bool {
		return uint64(rIDs[i]) < uint64(rIDs[j])
	})

	s := make([]string, len(rIDs))
	for i, rID := range rIDs {
		ri := n.ranges[rID]
		s[i] = fmt.Sprintf("%s:%s", ri.Meta.Ident, ri.State)
	}

	return fmt.Sprintf("{%s [%s]}", n.Ident(), strings.Join(s, " "))
}

func (n *Node) Get(rangeID api.RangeID) (api.RangeInfo, bool) {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()

	ri, ok := n.ranges[rangeID]
	if !ok {
		return api.RangeInfo{}, false
	}

	return *ri, true
}

func (n *Node) Ident() api.NodeID {
	return n.Remote.NodeID()
}

func (n *Node) Addr() string {
	return n.Remote.Addr()
}

func (n *Node) String() string {
	return fmt.Sprintf("N{%s}", n.Ident())
}

func (n *Node) IsGoneFromServiceDiscovery(now time.Time) bool {
	return n.whenLastSeen.Before(now.Add(-10 * time.Second))
}

// IsMissing returns true if this node hasn't responded to a probe in long
// enough that we think it's dead, and should move its ranges elsewhere.
func (n *Node) IsMissing(expireDuration time.Duration, now time.Time) bool {
	return (!n.whenLastProbed.IsZero()) && n.whenLastProbed.Before(now.Add(-expireDuration))
}

// Utilization returns a uint in [0, 255], indicating how busy this node is.
// Ranges should generally be placed on nodes with lower utilization.
func (n *Node) Utilization() uint8 {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()

	l := len(n.ranges)
	if l > 255 {
		return 255
	}

	return uint8(l) // lol
}

func (n *Node) WantDrain() bool {
	// TODO: Use a differet lock for this!
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()
	return n.wantDrain
}

// HasRange returns whether we think this node has the given range.
func (n *Node) HasRange(rID api.RangeID) bool {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()
	ri, ok := n.ranges[rID]

	// Note that if we have an entry for the range, but it's NsNotFound, that
	// means that the node told us (in response to a command RPC) that it does
	// NOT have that range. I don't remember why we do that as opposed to clear
	// the range state.
	// TODO: Find out why and update this comment. Might be obsolete.

	return ok && !(ri.State == api.NsNotFound)
}

func (n *Node) PlacementFailed(rID api.RangeID, t time.Time) {
	n.muPF.Lock()
	defer n.muPF.Unlock()
	n.placementFailures = append(n.placementFailures, PlacementFailure{rID: rID, when: t})
}

func (n *Node) PlacementFailures(rID api.RangeID, after time.Time) int {
	n.muPF.RLock()
	defer n.muPF.RUnlock()

	c := 0
	for _, pf := range n.placementFailures {
		if rID != api.ZeroRange && pf.rID != rID {
			continue
		}
		if pf.when.Before(after) {
			continue
		}

		c += 1
	}

	return c
}
