package fake_node

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type Action uint8

const (
	Prepare Action = iota
	AddRange
	PrepareDropRange
	DropRange
)

type stateTransition struct {
	src api.RemoteState
	bar *barrier
}

type ErrKey struct {
	rID api.RangeID
	act Action
}

type TestNode struct {
	Addr string
	Conn *grpc.ClientConn
	rglt *rangelet.Rangelet

	loadInfos map[api.RangeID]api.LoadInfo
	muInfos   sync.Mutex

	// Keep requests sent to this node.
	// Call RPCs() to fetch and clear.
	rpcs   []interface{}
	rpcsMu sync.Mutex

	// Barriers to block on in the middle of range state changes. This allows
	// tests to control exactly how long the interface methods (Prepare,
	// AddRange, etc) take to return.
	barriers map[api.RangeID]*stateTransition
	muBar    sync.Mutex

	// Errors (or nils) which should be injected into range state changes.
	errors map[ErrKey]error
	muErr  sync.Mutex

	// Panic unless a transition was registred.
	strictTransitions bool
}

func NewTestNode(ctx context.Context, addr string, rangeInfos map[api.RangeID]*api.RangeInfo) (*TestNode, func()) {

	// Extract LoadInfos to keep in the client (TestNode). Rangelet fetches via
	// GetLoadInfo.
	li := map[api.RangeID]api.LoadInfo{}
	for _, ri := range rangeInfos {
		li[ri.Meta.Ident] = api.LoadInfo{
			Keys: int(ri.Info.Keys),
		}
	}

	n := &TestNode{
		Addr:      addr,
		loadInfos: li,
		barriers:  map[api.RangeID]*stateTransition{},
		errors:    map[ErrKey]error{},
	}

	srv := grpc.NewServer()
	stor := fake_storage.NewFakeStorage(rangeInfos)
	n.rglt = rangelet.New(n, srv, stor)

	// Just for tests.
	n.rglt.SetGracePeriod(10 * time.Millisecond)

	closer := n.Listen(ctx, srv)

	return n, closer
}

// Rangelet has registered the NodeService by now.
func (n *TestNode) Listen(ctx context.Context, srv *grpc.Server) func() {
	conn, closer := n.nodeServer(ctx, srv)
	n.Conn = conn
	return closer
}

func (n *TestNode) transition(rID api.RangeID, act Action) error {
	n.muBar.Lock()

	ek := ErrKey{
		rID: rID,
		act: act,
	}

	n.muErr.Lock()
	e := n.errors[ek] // Might be nil; that's okay.
	n.muErr.Unlock()

	// Check whether a barrier has been registered for this transition. If so,
	// then notify it that the transition has been initiated (i.e. the fake node
	// has received an RPC and is ready to transition out of the src state). (If
	// we didn't do this, the test thread may try to assert the state of the
	// fake node before the rangelet has even received the RPC.)
	tr, ok := n.barriers[rID]
	if ok {
		delete(n.barriers, rID)
		n.muBar.Unlock()
		tr.bar.Arrive()
		return e
	}

	n.muBar.Unlock()

	if n.strictTransitions {
		panic(fmt.Sprintf("no transition registered for range while strict transitions enabled (n=%v, rID=%v, act=%d)", n.Addr, rID, act))
	}

	return e
}

func (n *TestNode) GetLoadInfo(rID api.RangeID) (api.LoadInfo, error) {
	n.muInfos.Lock()
	defer n.muInfos.Unlock()

	li, ok := n.loadInfos[rID]
	if !ok {
		return api.LoadInfo{}, api.ErrNotFound
	}

	return li, nil
}

func (n *TestNode) Prepare(m api.Meta, p []api.Parent) error {
	return n.transition(m.Ident, Prepare)
}

func (n *TestNode) AddRange(rID api.RangeID) error {
	return n.transition(rID, AddRange)
}

func (n *TestNode) PrepareDropRange(rID api.RangeID) error {
	return n.transition(rID, PrepareDropRange)
}

func (n *TestNode) DropRange(rID api.RangeID) error {
	// TODO: Remove placement from loadinfos if transition succeeds?
	return n.transition(rID, DropRange)
}

// From: https://harrigan.xyz/blog/testing-go-grpc-server-using-an-in-memory-buffer-with-bufconn/
func (n *TestNode) nodeServer(ctx context.Context, s *grpc.Server) (*grpc.ClientConn, func()) {
	listener := bufconn.Listen(1024 * 1024)

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), n.withTestInterceptor())

	return conn, s.Stop
}

func (n *TestNode) testInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {

	// Spy

	// TODO: Hack! Remove this! Update the tests to expect Info requests!
	if method != "/ranger.Node/Info" {
		n.rpcsMu.Lock()
		n.rpcs = append(n.rpcs, req)
		n.rpcsMu.Unlock()
	}

	err := invoker(ctx, method, req, res, cc, opts...)
	return err
}

func (n *TestNode) withTestInterceptor() grpc.DialOption {
	return grpc.WithUnaryInterceptor(n.testInterceptor)
}

// SetReturnValue sets the value which should be returned from the given action
// (i.e. one of the state-transitioning methods of the Rangelet interface) for
// the given range on this test node.
func (n *TestNode) SetReturnValue(t *testing.T, rID api.RangeID, act Action, err error) {
	ek := ErrKey{
		rID: rID,
		act: act,
	}

	n.muErr.Lock()
	n.errors[ek] = err
	n.muErr.Unlock()
}

func (n *TestNode) AddBarrier(t *testing.T, rID api.RangeID, src api.RemoteState) *barrier {
	desc := fmt.Sprintf("node=%s, range=%s, src=%s", n.Addr, rID.String(), src.String())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	n.rglt.OnLeaveState(rID, src, func() {
		wg.Done()
	})

	st := &stateTransition{
		src: src,
		bar: NewBarrier(desc, 1, func() {
			wg.Wait()
		}),
	}

	n.muBar.Lock()
	n.barriers[rID] = st
	n.muBar.Unlock()

	return st.bar
}

func (n *TestNode) ForceDrop(rID api.RangeID) {
	n.muInfos.Lock()
	defer n.muInfos.Unlock()
	delete(n.loadInfos, rID)
	n.rglt.ForceDrop(rID)
}

func (n *TestNode) Ranges(ctx context.Context, req *pb.RangesRequest) (*pb.RangesResponse, error) {
	panic("not imlemented!")
}

// RPCs returns a slice of the (proto) requests received by this node since the
// last time that this method was called, in a deterministic-ish order. This is
// an ugly hack because asserting that an unordered bunch of protos were all
// sent is hard.
func (n *TestNode) RPCs() []interface{} {
	n.rpcsMu.Lock()
	defer n.rpcsMu.Unlock()

	ret := n.rpcs
	n.rpcs = nil

	val := func(i int) int {

		switch v := ret[i].(type) {
		case *pb.PrepareRequest:
			return 100 + int(v.Range.Ident)

		case *pb.ServeRequest:
			return 200 + int(v.Range)

		case *pb.TakeRequest:
			return 300 + int(v.Range)

		case *pb.DropRequest:
			return 400 + int(v.Range)

		case *pb.InfoRequest:
			return 500

		default:
			panic(fmt.Sprintf("unhandled type: %T\n", v))
		}
	}

	// Sort by the Ident of the Range.
	sort.Slice(ret, func(i, j int) bool {
		return val(i) < val(j)
	})

	return ret
}

func (n *TestNode) SetWantDrain(b bool) {
	n.rglt.SetWantDrain(b)
}

func (n *TestNode) SetStrictTransitions(b bool) {
	n.strictTransitions = b
}

func (n *TestNode) SetGracePeriod(d time.Duration) {
	n.rglt.SetGracePeriod(d)
}
