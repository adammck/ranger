package roster

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"google.golang.org/grpc"
)

type Node struct {
	Remote discovery.Remote

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// When this node was last seen in service discovery. Doesn't necessarily
	// mean that it's actually alive, though.
	whenLastSeen time.Time

	// When this node was last successfully whenLastProbed. This means that it's
	// actually up and healthy enough to respond.
	whenLastProbed time.Time

	conn   *grpc.ClientConn
	client pb.NodeClient
	muConn sync.RWMutex

	// Populated by probeOne
	wantDrain bool
	ranges    map[ranje.Ident]*info.RangeInfo
	muRanges  sync.RWMutex

	// Only for tests: if non-null, nodes should send an RpcRecord every time
	// they send an RPC.
	RpcSpy chan RpcRecord
}

func NewNode(remote discovery.Remote, conn *grpc.ClientConn) *Node {
	return &Node{
		Remote:       remote,
		init:         time.Now(),
		whenLastSeen: time.Time{}, // never
		conn:         conn,
		client:       pb.NewNodeClient(conn),
		ranges:       make(map[ranje.Ident]*info.RangeInfo),
	}
}

// TODO: This is only used by tests. Maybe move it there?
func (n *Node) TestString() string {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()

	rIDs := []ranje.Ident{}
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

	return fmt.Sprintf("{%s [%s]}", n.Remote.Ident, strings.Join(s, ", "))
}

func (n *Node) Get(rangeID ranje.Ident) (info.RangeInfo, bool) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()

	ri, ok := n.ranges[rangeID]
	if !ok {
		return info.RangeInfo{}, false
	}

	return *ri, true
}

func (n *Node) Ident() string {
	return n.Remote.Ident
}

func (n *Node) Addr() string {
	return n.Remote.Addr()
}

func (n *Node) String() string {
	return fmt.Sprintf("N{%s}", n.Remote.Ident)
}

func (n *Node) IsGoneFromServiceDiscovery(cfg config.Config, now time.Time) bool {
	return n.whenLastSeen.Before(now.Add(-10 * time.Second))
}

// IsMissing returns true if this node hasn't responded to a probe in long
// enough that we think it's dead, and should move its ranges elsewhere.
func (n *Node) IsMissing(cfg config.Config, now time.Time) bool {
	return (!n.whenLastProbed.IsZero()) && n.whenLastProbed.Before(now.Add(-cfg.NodeExpireDuration))
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
func (n *Node) HasRange(rID ranje.Ident) bool {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()
	_, ok := n.ranges[rID]
	return ok
}
