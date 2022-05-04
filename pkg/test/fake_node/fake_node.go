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

type rangeInState struct {
	rID ranje.Ident
	s   state.RemoteState
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

	// Allow requests to be blocked by method name.
	// (This is to test what happens when RPCs span ticks.)
	gates   map[string][2]*sync.WaitGroup
	gatesMu sync.Mutex

	transitions   map[rangeInState]error
	transitionsMu sync.Mutex
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
		gates:       map[string][2]*sync.WaitGroup{},
		transitions: map[rangeInState]error{},
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

func (n *TestNode) waitUntil(rID ranje.Ident, src state.RemoteState) error {
	ris := rangeInState{
		rID: rID,
		s:   src,
	}

	t := time.Now().Add(5 * time.Second)

	for {
		n.transitionsMu.Lock()
		err, ok := n.transitions[ris]
		if ok {
			log.Printf("advancing %v!\n", rID)
			delete(n.transitions, ris)
		}
		n.transitionsMu.Unlock()
		if ok {
			return err
		}

		// TODO: This is flaky when the race detector is enabled. Replace this
		//       dumb double-sleeping synchronization with waitgroups.
		if time.Now().After(t) {
			panic(fmt.Sprintf("test deadlocked waiting for %v to advance past %v on node %v", ris.rID, ris.s, n.Addr))
		}

		time.Sleep(10 * time.Millisecond)
	}
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

	// Gate

	n.gatesMu.Lock()
	wgs, ok := n.gates[method]
	if ok {
		delete(n.gates, method)
	}
	n.gatesMu.Unlock()

	if ok {
		// TODO: Allow errors to be injected here.
		log.Printf("Gating RPC: method=%s", method)
		wgs[1].Done()
		wgs[0].Wait()
	}

	err := invoker(ctx, method, req, res, cc, opts...)
	log.Printf("RPC: method=%s; Error=%v", method, err)
	return err
}

func (n *TestNode) withTestInterceptor() grpc.DialOption {
	return grpc.WithUnaryInterceptor(n.testInterceptor)
}

func (n *TestNode) GateRPC(t *testing.T, method string) [2]*sync.WaitGroup {

	one := &sync.WaitGroup{}
	two := &sync.WaitGroup{}
	wgs := [2]*sync.WaitGroup{one, two}

	n.gatesMu.Lock()
	defer n.gatesMu.Unlock()

	if _, ok := n.gates[method]; ok {
		t.Fatalf("tried to add gate when one already exists: addr=%s, method=%s", n.Addr, method)
		return wgs
	}

	wgs[0].Add(1)

	log.Printf("Added gate: addr=%s, method=%s", n.Addr, method)
	n.gates[method] = wgs
	return wgs
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

	// Check that an error was given, if one was expected
	if wantErr && err != nil {
		t.Fatalf("need error to advance from %s to %s", exp, new)
		return

	} else if err != nil && !wantErr {
		t.Fatalf("don't need and error to advance from %s to %s", exp, new)
		return
	}

	ris := rangeInState{
		rID: rID,
		s:   exp,
	}

	log.Printf("registering transition: (rID=%v, state=%s)\n", ris.rID, ris.s)

	n.transitionsMu.Lock()
	n.transitions[ris] = err
	n.transitionsMu.Unlock()

	// Now wait until it's gone.
	// This assumes that some other goroutine is sitting in waitUntil.
	// TODO: Verify that before registering the transition. Fail the test if
	//       we try to advance while nobody is waiting.
	for {
		n.transitionsMu.Lock()
		_, ok := n.transitions[ris]
		n.transitionsMu.Unlock()

		if !ok {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	log.Println("advanced!")
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
