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

	// TODO: Get this stupid RpcSpy out of the actual code and into this file,
	//       maybe call back from the handlers in TestNode.
	spy := NewRpcSpy()
	rost.RpcSpy = spy.c
	spy.Start()
	defer spy.Stop()

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
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())
	// TODO: Assert that new placement was persisted

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())

	// The node finished preparing, but we don't know about it.
	nodes.RangeState("test-aaa", 1, roster.NsPrepared)
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Serve, Node: "test-aaa", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReadying]}", rost.TestString())

	// The node became ready, but as above, we don't know about it.
	nodes.RangeState("test-aaa", 1, roster.NsReady)
	assert.Equal(t, "{test-aaa [1:NsReadying]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Serve, Node: "test-aaa", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", rost.TestString())

	// Range Move
	// TODO: Split this into a separate test!

	nodes.Add(ctx, discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	})

	rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb []}", rost.TestString())

	p := r.Placements[0]
	p.WantMove = true

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb []}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Give, Node: "test-bbb", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPending}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", rost.TestString())

	// Node B finished preparing.
	nodes.RangeState("test-bbb", 1, roster.NsPrepared)
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", rost.TestString())

	rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move p1=test-bbb:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", rost.TestString())

	// Node A finished taking.
	nodes.RangeState("test-aaa", 1, roster.NsTaken)
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", rost.TestString())

	rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsPrepared}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", rost.TestString())

	// Node B finished becoming ready.
	nodes.RangeState("test-bbb", 1, roster.NsReady)
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", rost.TestString())

	rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, spy.Get())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", rost.TestString())

	nodes.FinishDrop(t, "test-aaa", 1)

	bal.Tick()
	assert.Equal(t, []roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, spy.Get()) // redundant
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move p1=test-bbb:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", rost.TestString())

	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped:want-move p1=test-bbb:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", rost.TestString())

	// test-aaa is gone!
	bal.Tick()
	assert.Equal(t, 0, len(spy.Get()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropped]} {test-bbb [1:NsReady]}", rost.TestString())

	rost.Tick()
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", rost.TestString())

	// Ensure that we are now in a stable state.
	ksLog := ks.LogString()
	rostLog := rost.TestString()
	for i := 0; i < 20; i++ {
		bal.Tick()
		rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		require.Zero(t, len(spy.Get()))
		require.Equal(t, ksLog, ks.LogString())
		require.Equal(t, rostLog, rost.TestString())
	}
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

func (tn *TestNodes) FinishDrop(t *testing.T, nID string, rID ranje.Ident) {
	n, ok := tn.nodes[nID]
	if !ok {
		t.Fatalf("no such node: %s", nID)
	}

	r, ok := n.ranges[rID]
	if !ok {
		t.Fatalf("can't drop unknown range: %s", rID)
		return
	}

	if r.info.State != roster.NsDropping {
		t.Fatalf("can't drop range not in NsDropping: rID=%s, state=%s", rID, r.info.State)
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
