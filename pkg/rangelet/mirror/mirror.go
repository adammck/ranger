// Package mirror provides some streaming grpc endpoints which clients can use
// to maintain a local mirror of the range assignments known to the roster. This
// is useful for services which don't provide a proxy or request relay between
// nodes; clients can embed a mirror and send requests directly to relevant
// node(s), without having to establish connections to every node.
//
// Clients could, alternatively, watch the persist storage for keyspace changes.
// However, that is currently considered a private interface. This is public.
//
// Note that this interface is eventually consistent, in that the orchestrator
// doesn't wait for clients to ack changes to the keyspace or anything like it.
// This interface doesn't even care what the orchestrator or even keyspace say;
// it simply reports what placements nodes report when probed or in response to
// actuations. The clients' mirror will thus always be a bit out of date.
package mirror

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

type node struct {
	closer chan bool
	conn   *grpc.ClientConn
	ranges []api.RangeInfo
}

// Result is returned by the Find method. Don't use it for anything else.
type Result struct {
	NodeID  api.NodeID
	RangeID api.RangeID
	State   api.RemoteState
}

type Dialler func(context.Context, discovery.Remote) (*grpc.ClientConn, error)

type Mirror struct {
	ctx  context.Context
	disc discovery.Getter

	// Dones holds a bunch of channels by NodeID. When a remote is added, a
	// goroutine is started (to fetch range assignments from the node) and a
	// channel is added to this map. To exit the goroutine, close the channel.
	nodes map[api.NodeID]*node

	// Dialler takes a remote and returns a gRPC client connection. This is only
	// parameterized for testing.
	dialler Dialler
}

func New(disc discovery.Discoverer, dialler Dialler) *Mirror {
	m := &Mirror{
		ctx:     context.Background(),
		nodes:   map[api.NodeID]*node{},
		dialler: dialler,
	}
	m.disc = disc.Discover("node", m.add, m.remove)
	return m
}

func (m *Mirror) add(rem discovery.Remote) {
	log.Printf("Adding: %s", rem.NodeID())

	conn, err := m.dialler(m.ctx, rem)
	if err != nil {
		log.Printf("Error dialling new remote: %v", err)
		return
	}

	done := make(chan bool, 1)

	m.nodes[rem.NodeID()] = &node{
		closer: done,
		conn:   conn,
	}

	go run(conn, done)
}

func (m *Mirror) remove(rem discovery.Remote) {
	nID := rem.NodeID()
	log.Printf("Removing: %s", nID)

	n, ok := m.nodes[nID]
	if !ok {
		return

	}

	close(n.closer)
	n.conn.Close()

	delete(m.nodes, nID)
}

var ErrNoSuchClientConn = errors.New("no such connection")

// Conn returns a connection to the given node ID. This is just a convenience
// for callers; the mirror needs to have a connection to every node in order to
// receive their range assignments, so callers may wish to reuse it to exchange
// other RPCs rather than creating a new one.
//
// Note that because the connection is owned by the Mirror, may be closed while
// the user is trying to use it. If that isn't acceptable, callers should manage
// their own connections.
func (m *Mirror) Conn(nID api.NodeID) (*grpc.ClientConn, bool) {
	n, ok := m.nodes[nID]
	return n.conn, ok
}

func (m *Mirror) FindConn(key api.Key, states ...api.RemoteState) []ResultConn {
}

func (m *Mirror) Find(key api.Key, states ...api.RemoteState) []Result {
	results := []Result{}

	//m.RLock()
	//defer m.RUnlock()

	// look i'm in a hurry here okay
	for nID, n := range m.nodes {
		func() {
			//n.muRanges.RLock()
			//defer n.muRanges.RUnlock()

			for _, ri := range n.ranges {
				if ri.Meta.Contains(key) {

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

					results = append(results, Result{
						NodeID:  nID,
						RangeID: ri.Meta.Ident,
						State:   ri.State,
					})
				}
			}
		}()
	}

	return results
}

func (m *Mirror) Stop() error {
	return m.disc.Stop()
}

func run(conn *grpc.ClientConn, done chan bool) {
	client := pb.NewNodeClient(conn)

	req := &pb.RangesRequest{}
	stream, err := client.Ranges(context.Background(), req)
	if err != nil {
		log.Printf("Error fetching ranges: %s", err)
		return
	}

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error fetching ranges: %s", err)
			break
		}

		log.Printf("res: %v", res)
	}
}
