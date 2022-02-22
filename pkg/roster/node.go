package roster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc"
)

const (
	staleTimer = 10 * time.Second
)

type Node struct {
	Remote discovery.Remote

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// time last seen in service discovery.
	seen time.Time

	conn   *grpc.ClientConn
	Client pb.NodeClient
	muConn sync.RWMutex

	// Populated by probeOne
	ranges   map[ranje.Ident]RangeInfo
	muRanges sync.RWMutex

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	//unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(remote discovery.Remote) *Node {
	n := Node{
		Remote: remote,
		init:   time.Now(),
		seen:   time.Time{}, // never
		ranges: make(map[ranje.Ident]RangeInfo),
	}

	// start dialling in background
	// todo: inherit context to allow global cancellation
	conn, err := grpc.DialContext(context.Background(), n.Remote.Addr(), grpc.WithInsecure())
	if err != nil {
		log.Printf("error while dialing: %v", err)
	}

	n.muConn.Lock()
	n.conn = conn
	n.Client = pb.NewNodeClient(n.conn)
	n.muConn.Unlock()

	return &n
}

func (n *Node) Get(rangeID ranje.Ident) State {
	info, ok := n.ranges[rangeID]

	if !ok {
		// TODO: Add a new state to represent this.
		return StateUnknown
	}

	return info.State
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

// Seen tells us that the node is still in service discovery.
// TODO: Combine this with the ShortNode somehow? Maybe it's fine.
func (n *Node) Seen(t time.Time) {
	n.seen = t
}

func (n *Node) IsStale(now time.Time) bool {
	return n.seen.Before(now.Add(-staleTimer))
}

func (n *Node) Conn() (grpc.ClientConnInterface, error) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	if n.conn == nil {
		return nil, errors.New("tried to read nil connection")
	}
	return n.conn, nil
}
