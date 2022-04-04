package balancer

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"context"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestInitial(t *testing.T) {
	ctx := context.Background()
	cfg := getConfig()

	nodes, closer := NewTestNodes()
	defer closer()

	nodes.Add(ctx, discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	})

	ks := ranje.New(cfg, &NullPersister{})
	rost := roster.New(cfg, nodes.Discovery(), nil, nil, nil)
	rost.NodeConnFactory = nodes.NodeConnFactory

	// Probe all of the fake nodes.
	rost.Tick()

	// TODO: Remove
	assert.Equal(t, 1, ks.Len())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", ks.LogString())
	// End

	// TODO: Move to keyspace tests.
	r := Get(t, ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	assert.Equal(t, ranje.RsActive, r.State, "range should be born active")
	assert.Equal(t, 0, len(r.Placements), "range should be born with no placements")

	srv := grpc.NewServer()
	bal := New(cfg, ks, rost, srv)
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", ks.LogString())
	assert.Equal(t, "{test-aaa []}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 1, bal.LastTickRPCs()) // Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())
	// TODO: Assert that new placement was persisted

	bal.Tick()
	assert.Equal(t, 1, bal.LastTickRPCs()) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())

	// The node finished preparing, but we don't know about it.
	nodes.RangeState("test-aaa", 1, roster.NsPrepared)
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 1, bal.LastTickRPCs()) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	bal.Tick()
	assert.Equal(t, 0, bal.LastTickRPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 1, bal.LastTickRPCs()) // Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReadying]}", rost.TestString())

	// The node became ready, but as above, we don't know about it.
	nodes.RangeState("test-aaa", 1, roster.NsReady)
	assert.Equal(t, "{test-aaa [1:NsReadying]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 1, bal.LastTickRPCs()) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 0, bal.LastTickRPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", rost.TestString())
}

// -----------------------------------------------------------------------------

func Get(t *testing.T, ks *ranje.Keyspace, rID uint64) *ranje.Range {
	r, err := ks.Get(ranje.Ident(rID))
	require.NoError(t, err)
	return r
}

func getConfig() config.Config {
	return config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
	}
}

// -----------------------------------------------------------------------------

type NullPersister struct {
}

func (fp *NullPersister) GetRanges() ([]*ranje.Range, error) {
	return []*ranje.Range{}, nil
}

func (fp *NullPersister) PutRanges([]*ranje.Range) error {
	return nil
}

// -----------------------------------------------------------------------------

type testRange struct {
	info *roster.RangeInfo
}

type TestNode struct {
	pb.UnimplementedNodeServer
	ranges map[ranje.Ident]*testRange
}

func NewTestNode() *TestNode {
	return &TestNode{
		ranges: map[ranje.Ident]*testRange{},
	}
}

func (n *TestNode) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	log.Printf("TestNode.Give")

	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	var info *roster.RangeInfo

	r, ok := n.ranges[meta.Ident]
	if !ok {
		info = &roster.RangeInfo{
			Meta:  *meta,
			State: roster.NsPreparing,
		}
		n.ranges[info.Meta.Ident] = &testRange{
			info: info,
		}
	} else {
		switch r.info.State {
		case roster.NsPreparing, roster.NsPreparingError, roster.NsPrepared:
			// We already know about this range, and it's in one of the states
			// that indicate a previous Give. This is a duplicate. Don't change
			// any state, just return the RangeInfo to let the controller know
			// how we're doing.
			info = r.info
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid state for redundant Give: %v", r.info.State)
		}
	}

	return &pb.GiveResponse{
		RangeInfo: info.ToProto(),
	}, nil
}

func (n *TestNode) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
	log.Printf("TestNode.Serve")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.ranges[rID]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	switch r.info.State {
	case roster.NsPrepared:
		r.info.State = roster.NsReadying

	case roster.NsReadying, roster.NsReady:
		log.Printf("got redundant Serve")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", r.info.State)
	}

	return &pb.ServeResponse{
		State: r.info.State.ToProto(),
	}, nil
}

func (n *TestNode) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	log.Printf("TestNode.Info")

	res := &pb.InfoResponse{
		WantDrain: false,
	}

	for _, r := range n.ranges {
		res.Ranges = append(res.Ranges, r.info.ToProto())
	}

	log.Printf("res: %v", res)
	return res, nil
}

// -----------------------------------------------------------------------------

type TestNodes struct {
	disc    *mockdisc.MockDiscovery
	nodes   map[string]*TestNode        // nID
	conns   map[string]*grpc.ClientConn // addr
	closers []func()
}

func NewTestNodes() (*TestNodes, func()) {
	tn := &TestNodes{
		disc:  mockdisc.New(),
		nodes: map[string]*TestNode{},
		conns: map[string]*grpc.ClientConn{},
	}

	return tn, tn.close
}

func (tn *TestNodes) close() {
	for _, f := range tn.closers {
		f()
	}
}

func (tn *TestNodes) Add(ctx context.Context, remote discovery.Remote) {
	n := NewTestNode()
	tn.nodes[remote.Ident] = n

	conn, closer := nodeServer(ctx, n)
	tn.conns[remote.Addr()] = conn
	tn.closers = append(tn.closers, closer)

	tn.disc.Add("node", remote)
}

func (tn *TestNodes) RangeState(nID string, rID ranje.Ident, state roster.RemoteState) {
	n, ok := tn.nodes[nID]
	if !ok {
		panic(fmt.Sprintf("no such node: %s", nID))
	}

	r, ok := n.ranges[rID]
	if !ok {
		panic(fmt.Sprintf("no such range: %s", rID))
	}

	log.Printf("RangeState: %s, %s -> %s", nID, rID, state)
	r.info.State = state
}

// Use this to stub out the Roster.
func (tn *TestNodes) NodeConnFactory(ctx context.Context, remote discovery.Remote) (*grpc.ClientConn, error) {
	conn, ok := tn.conns[remote.Addr()]
	if !ok {
		return nil, fmt.Errorf("no such connection: %v", remote.Addr())
	}

	return conn, nil
}

func (tn *TestNodes) Discovery() *mockdisc.MockDiscovery {
	return tn.disc
}

func nodeServer(ctx context.Context, node *TestNode) (*grpc.ClientConn, func()) {
	listener := bufconn.Listen(1024 * 1024)

	s := grpc.NewServer()
	pb.RegisterNodeServer(s, node)
	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())

	closer := func() {
		listener.Close()
		s.Stop()
	}

	return conn, closer
}
