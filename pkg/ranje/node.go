package ranje

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

const (
	staleTimer = 10 * time.Second
)

type Node struct {
	host string
	port int

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// time last seen in service discovery.
	seen time.Time

	conn   *grpc.ClientConn
	client pb.NodeClient
	muConn sync.RWMutex

	// The ranges that this node has. Populated via Probe.
	ranges map[Ident]*Placement

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(host string, port int) *Node {
	n := Node{
		host: host,
		port: port,
		init: time.Now(),
		seen: time.Time{}, // never
	}

	// start dialling in background
	//zap.L().Info("dialing...", zap.String("addr", n.addr))
	// todo: inherit context to allow global cancellation
	conn, err := grpc.DialContext(context.Background(), fmt.Sprintf("%s:%d", n.host, n.port), grpc.WithInsecure())
	if err != nil {
		//zap.L().Info("error while dialing", zap.String("addr", n.addr), zap.Error(err))
		fmt.Printf("error while dialing: %v\n", err)
	}

	n.muConn.Lock()
	n.conn = conn
	n.client = pb.NewNodeClient(n.conn)
	n.muConn.Unlock()

	return &n
}

// TODO: Replace this with a statusz-type page
func (n *Node) DumpForDebug() {
	for id, p := range n.ranges {
		fmt.Printf("   - %s %s\n", id, p.state.String())
	}
}

// Seen tells us that the node is still in service discovery.
func (n *Node) Seen(t time.Time) {
	n.seen = t
}

func (n *Node) IsStale(now time.Time) bool {
	return n.seen.Before(now.Add(-staleTimer))
}

func (n *Node) addr() string {
	return fmt.Sprintf("%s:%d", n.host, n.port)
}

func (n *Node) Give(id Ident, r *Range) error {
	_, ok := n.ranges[id]
	if ok {
		// Note that this doesn't check the *other* nodes, only this one
		return fmt.Errorf("range already given to node %s: %s", n.addr(), id.String())
	}

	n.ranges[id] = &Placement{
		rang:  r,
		node:  n,
		state: StateUnknown,
		K:     0,
	}

	return nil
}

// Probe updates current state of the node via RPC.
// Returns error if the RPC fails or if a probe is already in progess.
func (n *Node) Probe(ctx context.Context) error {
	// TODO: Abort if probe in progress.

	res, err := n.client.Info(ctx, &pb.InfoRequest{})
	if err != nil {
		fmt.Printf("Probe failed: %s\n", err)
		return err
	}

	for _, r := range res.Ranges {
		rr := r.Range
		if rr == nil {
			fmt.Printf("Malformed probe response from node %s: Range is nil\n", n.addr())
			continue
		}

		id, err := IdentFromProto(rr.Ident)
		if err != nil {
			fmt.Printf("Got malformed ident from node %s: %s\n", n.addr(), err.Error())
			continue
		}

		rrr, ok := n.ranges[*id]

		if !ok {
			fmt.Printf("Got unexpected range from node %s: %s\n", n.addr(), id.String())
			n.unexpectedRanges[*id] = rr
			continue
		}

		// TODO: We compare the Ident here even though we just fetched the assignment by ID. Is that... why
		if !rrr.rang.SameMeta(*id, rr.Start, rr.End) {
			fmt.Printf("Remote range did not match local range with same ident: %s\n", id.String())
			continue
		}

		// Finally update the remote info
		rrr.K = r.Keys
		rrr.state = RemoteStateFromProto(r.State)
	}

	return nil
}

// Drop cleans up the node. Called when it hasn't responded to probes in a long time.
func (n *Node) Drop() {
	n.muConn.Lock()
	defer n.muConn.Unlock()
	n.conn.Close()
}

func (n *Node) Conn() (grpc.ClientConnInterface, error) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	if n.conn == nil {
		return nil, errors.New("tried to read nil connection")
	}
	return n.conn, nil
}

func (n *Node) Connected() bool {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	return n.conn != nil
}
