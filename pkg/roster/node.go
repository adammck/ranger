package roster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/ident"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

const (
	probeTimeout = 3 * time.Second
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

	ranges map[ident.Ident]*keyspace.Range

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	unexpectedRanges map[ident.Ident]*pb.Range
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

func (n *Node) addr() string {
	return fmt.Sprintf("%s:%d", n.host, n.port)
}

// Probe updates current state of the node via RPC.
// Returns error if the RPC fails or if a probe is already in progess.
func (n *Node) Probe() error {
	// TODO: Abort if probe in progress.

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

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

		id := ident.FromProto(rr.Ident)
		rrr, ok := n.ranges[id]

		if !ok {
			fmt.Printf("Got unexpected range from node %s: %s\n", n.addr(), id)
			n.unexpectedRanges[id] = rr
			continue
		}

		_ = rrr
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
