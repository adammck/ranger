// Package mirror provides a range assignment mirror, which can maintain a map
// of all range assignments via the streaming Node.Ranges endpoint provided by
// the rangelet. This is useful for proxies and clients wishing to forward
// requests to the relevant node(s).
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
	"sync"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

// Result is returned by the Find method. Don't use it for anything else.
type Result struct {
	NodeID  api.NodeID
	RangeID api.RangeID
	State   api.RemoteState
}

type Dialler func(context.Context, api.Remote) (*grpc.ClientConn, error)

type Mirror struct {
	ctx  context.Context
	disc discovery.Getter

	nodes   map[api.NodeID]*node
	nodesMu sync.RWMutex

	// Dialler takes a remote and returns a gRPC client connection. This is only
	// parameterized for testing.
	dialler Dialler
}

type node struct {
	closer   chan bool
	conn     *grpc.ClientConn
	ranges   []api.RangeInfo
	rangesMu sync.RWMutex
}

func (n *node) stop() {
	//close(n.closer)
	n.conn.Close()
}

func New(disc discovery.Discoverer) *Mirror {
	m := &Mirror{
		ctx:   context.Background(),
		nodes: map[api.NodeID]*node{},
	}
	m.disc = disc.Discover("node", m.add, m.remove)
	return m
}

func (m *Mirror) WithDialler(d Dialler) *Mirror {
	m.dialler = d
	return m
}

func (m *Mirror) add(rem api.Remote) {
	log.Printf("Adding: %s", rem.NodeID())

	conn, err := m.dialler(m.ctx, rem)
	if err != nil {
		log.Printf("Error dialling new remote: %v", err)
		return
	}

	n := &node{
		closer: make(chan bool, 1),
		conn:   conn,
	}

	m.nodesMu.Lock()
	m.nodes[rem.NodeID()] = n
	m.nodesMu.Unlock()

	go run(conn, n)
}

func (m *Mirror) remove(rem api.Remote) {
	nID := rem.NodeID()
	log.Printf("Removing: %s", nID)

	m.nodesMu.Lock()

	n, ok := m.nodes[nID]
	if !ok {
		m.nodesMu.Unlock()
		return
	}

	delete(m.nodes, nID)

	// Release and let closers happen outside the lock.
	m.nodesMu.Unlock()

	n.stop()
}

func (m *Mirror) Stop() error {

	err := m.disc.Stop()
	if err != nil {
		// Not ideal, but we still want to stop the nodes.
		log.Printf("Error stopping discovery getter: %s", err)
	}

	m.nodesMu.Lock()
	defer m.nodesMu.Unlock()

	// Call all closers concurrently.
	wg := sync.WaitGroup{}
	for k := range m.nodes {
		wg.Add(1)
		n := m.nodes[k]
		go func() {
			n.stop()
			wg.Done()
		}()
	}
	wg.Wait()

	m.nodes = map[api.NodeID]*node{}

	return nil
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
	m.nodesMu.RLock()
	n, ok := m.nodes[nID]
	m.nodesMu.RUnlock()
	return n.conn, ok
}

func (m *Mirror) Find(key api.Key, states ...api.RemoteState) []Result {
	results := []Result{}

	m.nodesMu.RLock()
	defer m.nodesMu.RUnlock()

	// look i'm in a hurry here okay
	for nID, n := range m.nodes {
		n.rangesMu.RLock()
		defer n.rangesMu.RUnlock()

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
	}

	return results
}

func run(conn *grpc.ClientConn, node *node) {
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

		update(node, res)
	}
}

func update(n *node, res *pb.RangesResponse) {
	log.Printf("res: %v", res)

	meta, err := conv.MetaFromProto(res.Meta)
	if err != nil {
		// This should never happen.
		log.Printf("Error parsing Range Meta from proto: %s", err)
		return
	}

	state := conv.RemoteStateFromProto(res.State)
	if state == api.NsUnknown {
		// This should also never happen.
		log.Printf("Error updating range state: got NsUnknown for rid=%s", meta.Ident)
		return
	}

	// TODO: This is pretty coarse, maybe optimize.
	n.rangesMu.Lock()
	defer n.rangesMu.Unlock()

	// Find the range by Range ID, and update the state. Meta is immutable after
	// range construction, so assume it hasn't changed. God help us if it has.
	for i := range n.ranges {
		if n.ranges[i].Meta.Ident == meta.Ident {
			if state == api.NsNotFound {
				// Special case: Remove the range if it's NotFound.
				x := len(n.ranges) - 1
				n.ranges[i] = n.ranges[x]
				n.ranges = n.ranges[:x]
			} else {
				// Normal case: Update the state.
				n.ranges[i].State = state
			}
			return
		}
	}

	// Haven't returned? First time we're seeing this range, so insert it unless
	// it was NotFound (which could happen if the proxy is starting up just as a
	// node is dropping a range, but probably never otherwise).

	if state == api.NsNotFound {
		return
	}

	n.ranges = append(n.ranges, api.RangeInfo{
		Meta:  meta,
		State: state,
	})
}
