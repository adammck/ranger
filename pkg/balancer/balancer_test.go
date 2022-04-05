package balancer

import (
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"context"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type BalancerSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *TestNodes
	ks    *ranje.Keyspace
	rost  *roster.Roster
	spy   *RpcSpy
	bal   *Balancer
}

// GetRange returns a range from the test's keyspace or fails the test.
func (ts *BalancerSuite) GetRange(rID uint64) *ranje.Range {
	r, err := ts.ks.Get(ranje.Ident(rID))
	ts.Require().NoError(err)
	return r
}

func (ts *BalancerSuite) SetupTest() {
	ts.ctx = context.Background()

	// Sensible defaults.
	// TODO: Better to set these per test, but that's too late because we've
	//       already created the objects in this method, and config is supposed
	//       to be immutable. Better rethink this.
	ts.cfg = config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
	}

	ts.nodes = NewTestNodes()

	ts.ks = ranje.New(ts.cfg, &NullPersister{})
	ts.rost = roster.New(ts.cfg, ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory

	// TODO: Get this stupid RpcSpy out of the actual code and into this file,
	//       maybe call back from the handlers in TestNode.
	ts.spy = NewRpcSpy()
	ts.rost.RpcSpy = ts.spy.c
	ts.spy.Start()

	srv := grpc.NewServer()
	ts.bal = New(ts.cfg, ts.ks, ts.rost, srv)
}

func (ts *BalancerSuite) TearDownTest() {
	if ts.nodes != nil {
		ts.nodes.Close()
	}
	if ts.spy != nil {
		ts.spy.Stop()
	}
}

func (ts *BalancerSuite) TestJunk() {
	// TODO: Move to keyspace tests.

	// TODO: Remove
	ts.Equal(1, ts.ks.Len())
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	// End

	r := ts.GetRange(1)
	ts.NotNil(r)
	ts.Equal(ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	ts.Equal(ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	ts.Equal(ranje.RsActive, r.State, "range should be born active")
	ts.Equal(0, len(r.Placements), "range should be born with no placements")
}

func (ts *BalancerSuite) TestPlacement() {
	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	})

	// Probe all of the fake nodes.
	ts.rost.Tick()

	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())
	// TODO: Assert that new placement was persisted

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	// The node finished preparing, but we don't know about it.
	ts.nodes.RangeState("test-aaa", 1, roster.NsPrepared)
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ts.nodes.RangeState("test-aaa", 1, roster.NsReady)
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	// Range Move
	// TODO: Split this into a separate test!

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	})

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	{
		r := ts.GetRange(1)
		p := r.Placements[0]
		p.WantMove = true
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	// Node B finished preparing.
	ts.nodes.RangeState("test-bbb", 1, roster.NsPrepared)
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Node A finished taking.
	ts.nodes.RangeState("test-aaa", 1, roster.NsTaken)
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	// Node B finished becoming ready.
	ts.nodes.RangeState("test-bbb", 1, roster.NsReady)
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.nodes.FinishDrop(ts, "test-aaa", 1)

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped:want-move p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// Ensure that we are now in a stable state.
	ksLog := ts.ks.LogString()
	rostLog := ts.rost.TestString()
	for i := 0; i < 20; i++ {
		ts.bal.Tick()
		ts.rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		ts.Require().Zero(len(ts.spy.Get()))
		ts.Require().Equal(ksLog, ts.ks.LogString())
		ts.Require().Equal(rostLog, ts.rost.TestString())
	}
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(BalancerSuite))
}

// -----------------------------------------------------------------------------

type RpcSpy struct {
	c   chan roster.RpcRecord
	buf []roster.RpcRecord
	sync.Mutex
}

func NewRpcSpy() *RpcSpy {
	return &RpcSpy{
		c: make(chan roster.RpcRecord),
	}
}

func (spy *RpcSpy) Start() {
	go func() {
		for rec := range spy.c {
			func() {
				spy.Lock()
				defer spy.Unlock()
				spy.buf = append(spy.buf, rec)
			}()
		}
	}()
}

func (spy *RpcSpy) Stop() {
	close(spy.c)
}

func (spy *RpcSpy) Get() []roster.RpcRecord {
	spy.Lock()
	defer spy.Unlock()
	buf := spy.buf
	spy.buf = nil
	return buf
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

// TODO: Most of this should be moved into a client library. Rangelet?
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
		// Actual state transition.
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

func (n *TestNode) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
	log.Printf("TestNode.Take")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.ranges[rID]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	switch r.info.State {
	case roster.NsReady:
		// Actual state transition.
		r.info.State = roster.NsTaking

	case roster.NsTaking, roster.NsTaken:
		log.Printf("got redundant Take")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", r.info.State)
	}

	return &pb.TakeResponse{
		State: r.info.State.ToProto(),
	}, nil
}

func (n *TestNode) Drop(ctx context.Context, req *pb.DropRequest) (*pb.DropResponse, error) {
	log.Printf("TestNode.Drop")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.ranges[rID]
	if !ok {
		// return nil, status.Errorf(codes.InvalidArgument, "can't Drop unknown range: %v", rID)
		return &pb.DropResponse{
			State: roster.NsDropped.ToProto(),
		}, nil
	}

	switch r.info.State {
	case roster.NsTaken:
		// Actual state transition. We don't actually drop anything here, only
		// claim that we are doing so, to simulate a slow client. Test must call
		// FinishDrop to move to NsDropped.
		r.info.State = roster.NsDropping

	case roster.NsDropping, roster.NsDropped:
		log.Printf("got redundant Drop")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", r.info.State)
	}

	return &pb.DropResponse{
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

func NewTestNodes() *TestNodes {
	tn := &TestNodes{
		disc:  mockdisc.New(),
		nodes: map[string]*TestNode{},
		conns: map[string]*grpc.ClientConn{},
	}

	return tn
}

func (tn *TestNodes) Close() {
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

func (tn *TestNodes) FinishDrop(ts *BalancerSuite, nID string, rID ranje.Ident) {
	n, ok := tn.nodes[nID]
	if !ok {
		ts.T().Fatalf("no such node: %s", nID)
	}

	r, ok := n.ranges[rID]
	if !ok {
		ts.T().Fatalf("can't drop unknown range: %s", rID)
		return
	}

	if r.info.State != roster.NsDropping {
		ts.T().Fatalf("can't drop range not in NsDropping: rID=%s, state=%s", rID, r.info.State)
		return
	}

	log.Printf("FinishDrop: nID=%s, rID=%s", nID, rID)
	delete(n.ranges, rID)
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
