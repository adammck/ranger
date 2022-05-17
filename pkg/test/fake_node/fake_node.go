package fake_node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type stateTransition struct {
	src state.RemoteState
	bar *barrier
}

type ErrKey struct {
	rID ranje.Ident
	src state.RemoteState
}

type TestNode struct {
	Addr string
	Conn *grpc.ClientConn
	rglt *rangelet.Rangelet

	loadInfos map[ranje.Ident]api.LoadInfo

	// Keep requests sent to this node.
	// Call RPCs() to fetch and clear.
	rpcs   []interface{}
	rpcsMu sync.Mutex

	barriers map[ranje.Ident]*stateTransition
	muBar    sync.Mutex

	// Errors (or nils) which should be injected into range state changes.
	errors map[ErrKey]error
	muErr  sync.Mutex

	// Panic unless a transition was registred.
	strictTransitions bool
}

func NewTestNode(ctx context.Context, addr string, rangeInfos map[ranje.Ident]*info.RangeInfo) (*TestNode, func()) {

	// Extract LoadInfos to keep in the client (TestNode). Rangelet fetches via
	// GetLoadInfo.
	li := map[ranje.Ident]api.LoadInfo{}
	for _, ri := range rangeInfos {
		li[ri.Meta.Ident] = api.LoadInfo{
			Keys: int(ri.Info.Keys),
		}
	}

	n := &TestNode{
		Addr:      addr,
		loadInfos: li,
		barriers:  map[ranje.Ident]*stateTransition{},
		errors:    map[ErrKey]error{},
	}

	srv := grpc.NewServer()
	stor := fake_storage.NewFakeStorage(rangeInfos)
	n.rglt = rangelet.NewRangelet(n, srv, stor)

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

func (n *TestNode) transition(rID ranje.Ident, src state.RemoteState) error {
	n.muBar.Lock()

	ek := ErrKey{
		rID: rID,
		src: src,
	}

	n.muErr.Lock()
	e := n.errors[ek] // Might be nil; that's okay.
	n.muErr.Unlock()

	// Check whether a transition is already pending for this range. If so, we
	// can return early without waiting.
	tr, ok := n.barriers[rID]
	if ok {
		delete(n.barriers, rID)
		n.muBar.Unlock()
		tr.bar.Arrive()
		return e
	}

	n.muBar.Unlock()

	if n.strictTransitions {
		panic(fmt.Sprintf("no transition registered for range while strict transitions enabled (rID=%v)", rID))
	}

	return e
}

func (n *TestNode) GetLoadInfo(rID ranje.Ident) (api.LoadInfo, error) {
	li, ok := n.loadInfos[rID]
	if !ok {
		return api.LoadInfo{}, api.NotFound
	}

	return li, nil
}

func (n *TestNode) PrepareAddRange(m ranje.Meta, p []api.Parent) error {
	return n.transition(m.Ident, state.NsPreparing)
}

func (n *TestNode) AddRange(rID ranje.Ident) error {
	return n.transition(rID, state.NsReadying)
}

func (n *TestNode) PrepareDropRange(rID ranje.Ident) error {
	return n.transition(rID, state.NsTaking)
}

func (n *TestNode) DropRange(rID ranje.Ident) error {
	return n.transition(rID, state.NsDropping)
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
	}), grpc.WithInsecure(), grpc.WithBlock(), n.withTestInterceptor())

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
	log.Printf("RPC: method=%s; Error=%v", method, err)
	return err
}

func (n *TestNode) withTestInterceptor() grpc.DialOption {
	return grpc.WithUnaryInterceptor(n.testInterceptor)
}

func (n *TestNode) SetReturnValue(t *testing.T, rID ranje.Ident, src state.RemoteState, err error) {
	ek := ErrKey{
		rID: rID,
		src: src,
	}

	n.muErr.Lock()
	defer n.muErr.Unlock()

	n.errors[ek] = err
}

func (n *TestNode) AddBarrier(t *testing.T, rID ranje.Ident, src state.RemoteState) *barrier {
	n.muBar.Lock()
	defer n.muBar.Unlock()

	// Don't allow overwrites. It's confusing.
	if tr, ok := n.barriers[rID]; ok {
		t.Fatalf("already have pending state transition (addr=%s, rID=%v, src=%v)", n.Addr, rID, tr.src)
		return nil
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	n.rglt.OnLeaveState(rID, src, func() {
		wg.Done()
	})

	st := &stateTransition{
		src: src,
		bar: NewBarrier(1, wg.Wait),
	}

	n.barriers[rID] = st

	return st.bar
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
		case *pb.GiveRequest:
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
