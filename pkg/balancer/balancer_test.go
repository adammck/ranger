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

	bal.Tick()
	// TODO: Assert that the Give RPC was sent
	// TODO: Assert that new placement was persisted
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())

	bal.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsLoading}", ks.LogString())

	// Check that nothing is changing.
	s := ks.LogString()
	for i := 0; i < 20; i++ {
		bal.Tick()
		assert.Equal(t, s, ks.LogString())
	}

	// The node finished loading the range.
	nodes.RangeState("test-aaa", 1, roster.StateFetched)

	// Still in PsLoading because the roster hasn't probed it yet, so the
	// controller doesn't know. Balancer ticks don't send probes.
	bal.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsLoading}", ks.LogString())

	rost.Tick()

	bal.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ks.LogString())
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
	ranges []testRange
}

func (n *TestNode) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	log.Printf("give")

	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	info := &roster.RangeInfo{
		Meta:  *meta,
		State: roster.StateFetching,
	}

	n.ranges = append(n.ranges, testRange{
		info: info,
	})

	return &pb.GiveResponse{
		RangeInfo: info.ToProto(),
	}, nil
}

func (n *TestNode) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	log.Printf("info")

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
	n := &TestNode{}
	tn.nodes[remote.Ident] = n

	conn, closer := nodeServer(ctx, n)
	tn.conns[remote.Addr()] = conn
	tn.closers = append(tn.closers, closer)

	tn.disc.Add("node", remote)
}

func (tn *TestNodes) RangeState(nID string, rID ranje.Ident, state roster.State) {
	n, ok := tn.nodes[nID]
	if !ok {
		panic(fmt.Sprintf("no such node: %s", nID))
	}

	for _, r := range n.ranges {
		if r.info.Meta.Ident == rID {
			log.Printf("RangeState: %s, %s -> %s", nID, rID, state)
			r.info.State = state
			return
		}
	}

	panic(fmt.Sprintf("no such range: %s", rID))
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
