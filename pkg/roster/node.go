package roster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
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
	Client pb.NodeClient
	muConn sync.RWMutex

	// Populated by probeOne
	wantDrain bool
	ranges    map[ranje.Ident]*RangeInfo
	muRanges  sync.RWMutex

	// to be cancelled
	// rpcs map[ranje.Ident]context.Context

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	//unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(remote discovery.Remote, conn *grpc.ClientConn) *Node {
	return &Node{
		Remote:       remote,
		init:         time.Now(),
		whenLastSeen: time.Time{}, // never
		conn:         conn,
		Client:       pb.NewNodeClient(conn),
		ranges:       make(map[ranje.Ident]*RangeInfo),
	}
}

func (n *Node) TestString() string {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()

	i := 0
	s := make([]string, len(n.ranges))
	for _, ri := range n.ranges {
		s[i] = fmt.Sprintf("%s:%s", ri.Meta.Ident, ri.State)
	}

	return fmt.Sprintf("{%s [%s]}", n.Remote.Ident, strings.Join(s, ", "))
}

func (n *Node) Get(rangeID ranje.Ident) (RangeInfo, bool) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()

	ri, ok := n.ranges[rangeID]
	if !ok {
		return RangeInfo{}, false
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
	return 255 // lol
}

func (n *Node) WantDrain() bool {
	n.muRanges.RLock()
	defer n.muRanges.RUnlock()
	return n.wantDrain
}

func (n *Node) Conn() (grpc.ClientConnInterface, error) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	if n.conn == nil {
		return nil, errors.New("tried to read nil connection")
	}
	return n.conn, nil
}
