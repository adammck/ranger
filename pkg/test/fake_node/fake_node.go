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
	err error
	bar *barrier
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

	transitions   map[ranje.Ident]*stateTransition
	transitionsMu sync.Mutex

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
		Addr:        addr,
		loadInfos:   li,
		transitions: map[ranje.Ident]*stateTransition{},
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

// waitUntil registers a transition (by rangeID) which we expect will happen
// in the future, and blocks until AdvanceTo(rID) is called in a different
// thread. It returns the error inserted into the transition by AdvanceTo.
func (n *TestNode) waitUntil(rID ranje.Ident, src state.RemoteState) error {
	n.transitionsMu.Lock()

	// Check whether a transition is already pending for this range. If so, we
	// can return early without waiting.
	tr, ok := n.transitions[rID]
	if ok {
		defer n.transitionsMu.Unlock()
		delete(n.transitions, rID)

		if tr.src != src {
			// TODO: Hang on to t.Testing in NewTestNode, so we can access it
			//       here and fail the test rather than panicking.
			panic(fmt.Sprintf("expected src state on waiting range to be %v, but was %v (rID=%v)", src, tr.src, rID))
		}

		// Note that we've arrived, and wait for testing thread to unblock us.
		tr.bar.Arrive()

		return tr.err
	}

	// No transition was registered, so we will either return no error, or
	// block.
	if !n.strictTransitions {
		n.transitionsMu.Unlock()
		return nil
	}

	bar := NewBarrier(1)

	// Register pending transition, so other thread (where AdvanceTo(rID, src)
	// will be called) can see that we're waiting, and unblock us.

	n.transitions[rID] = &stateTransition{
		err: nil,
		src: src,
		bar: bar,
	}

	// Block until other thread calls wg.Done.
	log.Printf("waiting on %v", rID)
	n.transitionsMu.Unlock()
	bar.Arrive()
	log.Printf("unblocked %v", rID)

	// AdvanceTo will have inserted the error that we should return.
	// Fetch it and clean up the transition.
	n.transitionsMu.Lock()
	err := n.transitions[rID].err
	delete(n.transitions, rID)
	n.transitionsMu.Unlock()

	return err
}

func (n *TestNode) GetLoadInfo(rID ranje.Ident) (api.LoadInfo, error) {
	li, ok := n.loadInfos[rID]
	if !ok {
		return api.LoadInfo{}, api.NotFound
	}

	return li, nil
}

func (n *TestNode) PrepareAddRange(m ranje.Meta, p []api.Parent) error {
	return n.waitUntil(m.Ident, state.NsPreparing)
}

func (n *TestNode) AddRange(rID ranje.Ident) error {
	return n.waitUntil(rID, state.NsReadying)
}

func (n *TestNode) PrepareDropRange(rID ranje.Ident) error {
	return n.waitUntil(rID, state.NsTaking)
}

func (n *TestNode) DropRange(rID ranje.Ident) error {
	return n.waitUntil(rID, state.NsDropping)
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

// TODO: Allow this to be called without returning (and using) a barrier, to
//       inject errors without blocking.
func (n *TestNode) Expect(t *testing.T, rID ranje.Ident, src state.RemoteState, err error) *barrier {
	bar := NewBarrier(1)
	st := &stateTransition{
		src: src,
		err: err,
		bar: bar,
	}

	n.transitionsMu.Lock()
	defer n.transitionsMu.Unlock()

	// Don't allow overwrites. It's confusing.
	if tr, ok := n.transitions[rID]; ok {
		t.Fatalf("already have pending state transition (addr=%s, rID=%v, src=%v)", n.Addr, rID, tr.src)
		return nil
	}

	n.transitions[rID] = st

	return bar
}

func (n *TestNode) AdvanceTo(t *testing.T, rID ranje.Ident, new state.RemoteState, err error) {
	var exp state.RemoteState
	var wantErr bool

	switch new {
	case state.NsPrepared:
		exp = state.NsPreparing
		wantErr = false

	case state.NsPreparingError:
		exp = state.NsPreparing
		wantErr = true

	case state.NsReady:
		exp = state.NsReadying
		wantErr = false

	case state.NsReadyingError:
		exp = state.NsReadying
		wantErr = true

	case state.NsTaken:
		exp = state.NsTaking
		wantErr = false

	case state.NsTakingError:
		exp = state.NsTaking
		wantErr = true

	case state.NsNotFound:
		exp = state.NsDropping
		wantErr = false

	case state.NsDroppingError:
		exp = state.NsDropping
		wantErr = true

	default:
		t.Fatalf("can't advance to state: %s", new)
	}

	// Check that an error was given, if one was expected.
	if wantErr && err != nil {
		t.Fatalf("need error to advance from %s to %s", exp, new)
		return
	} else if err != nil && !wantErr {
		t.Fatalf("don't need error to advance from %s to %s", exp, new)
		return
	}

	// Expect that some other thread is waiting on tr.wg in waitUntil. Fail the
	// whole test if that isn't the case, to avoid waiting forever.
	n.transitionsMu.Lock()
	tr, ok := n.transitions[rID]
	n.transitionsMu.Unlock()
	if !ok {
		t.Fatalf("expected range to be waiting (rID=%v)", rID)
		return
	}
	if tr.src != exp {
		t.Fatalf("expected src state on waiting range to be %v, but was %v (rID=%v)", exp, tr.src, rID)
		return
	}

	// Register a callback with the rangelet. To return from this helper, we
	// can't just wait for waitUntil to return, we must wait a little longer
	// until the rangelet has updated the state for that range. If we return
	// too soon, a probe might be sent and processed before the other thread as
	// actually updated the state.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	n.rglt.OnState(rID, new, func() {
		wg.Done()
	})

	// Insert the error (maybe nil) that we want the other thread to return, and
	// unblock it.
	n.transitionsMu.Lock()
	tr.err = err
	n.transitionsMu.Unlock()
	tr.bar.Release()

	// Wait for that callback to be called, indicating that the rangelet has
	// updated the state. After that, we can be sure that probes and such will
	// see the state that we advanced to.
	log.Printf("waiting for range to enter state (rID=%v, s=%s)", rID, new)
	wg.Wait()
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
