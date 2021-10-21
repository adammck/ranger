package roster2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc"
)

type ShortNode struct {
	remote discovery.Remote

	conn   *grpc.ClientConn
	client pb.NodeClient

	// Populated by probeOne
	ranges   map[ranje.Meta]ranje.StateRemote
	muRanges sync.RWMutex

	// Updated by discover
	seen time.Time
}

const (
	staleTimer = 10 * time.Second
)

func NewShortNode(rem discovery.Remote) *ShortNode {
	n := &ShortNode{
		remote: rem,
		ranges: make(map[ranje.Meta]ranje.StateRemote),
	}

	// Dial in background.
	conn, err := grpc.DialContext(context.Background(), n.remote.Addr(), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("error while dialing: %v\n", err)
		return nil
	}

	//n.muConn.Lock()
	n.conn = conn
	n.client = pb.NewNodeClient(conn)
	//n.muConn.Unlock()

	return n
}

func (n *ShortNode) IsStale(now time.Time) bool {
	return n.seen.Before(now.Add(-staleTimer))
}

// Seen tells us that the node is still in service discovery.
func (n *ShortNode) Seen(t time.Time) {
	n.seen = t
}
