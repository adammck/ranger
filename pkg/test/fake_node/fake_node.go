package fake_node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"testing"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type rangeInState struct {
	rID ranje.Ident
	s   state.RemoteState
}

type TestNode struct {
	Addr string
	srv  *grpc.Server // TODO: Do we need to keep this?
	Conn *grpc.ClientConn
	rpcs []interface{}

	rglt *rangelet.Rangelet

	// TODO: Locking.
	transitions map[rangeInState]error
}

func NewTestNode(ctx context.Context, addr string, rangeInfos map[ranje.Ident]*info.RangeInfo) (*TestNode, func()) {
	infos := []*info.RangeInfo{}
	for _, ri := range rangeInfos {
		infos = append(infos, ri)
	}

	srv := grpc.NewServer()

	tn := &TestNode{
		Addr:        addr,
		srv:         srv,
		transitions: map[rangeInState]error{},
	}

	stor := Storage{infos: infos}
	rglt := rangelet.NewRangelet(tn, srv, &stor)

	// Rangelet has registered the NodeService by now.
	conn, closer := tn.nodeServer(ctx, srv)
	tn.Conn = conn

	// TODO: Does the client actually need to talk back to the rangelet other
	//       than via the callbacks? Probably for to update health of ranges?
	//       but maybe that can just happen via the rangeinfos.
	tn.rglt = rglt

	return tn, closer
}

func (n *TestNode) waitUntil(rID ranje.Ident, src state.RemoteState) error {
	ris := rangeInState{
		rID: rID,
		s:   src,
	}

	t := time.Now().Add(500 * time.Millisecond)

	for {
		if err, ok := n.transitions[ris]; ok {
			log.Printf("advancing %v!\n", rID)
			delete(n.transitions, ris)
			return err
		}

		if time.Now().After(t) {
			panic(fmt.Sprintf("test deadlocked waiting for %v to advance past %v on node %v", ris.rID, ris.s, n.Addr))
		}

		log.Println("zzz...")
		time.Sleep(1 * time.Millisecond)
	}
}

func (n *TestNode) PrepareAddShard(m ranje.Meta) error {
	return n.waitUntil(m.Ident, state.NsPreparing)
}

func (n *TestNode) AddShard(rID ranje.Ident) error {
	return n.waitUntil(rID, state.NsReadying)
}

func (n *TestNode) PrepareDropShard(rID ranje.Ident) error {
	return n.waitUntil(rID, state.NsTaking)
}

func (n *TestNode) DropShard(rID ranje.Ident) error {
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
	}), grpc.WithInsecure(), grpc.WithBlock(), n.withSpyInterceptor())

	return conn, s.Stop
}

func (n *TestNode) spyInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	res interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {

	// TODO: Hack! Remove this! Update the tests to expect Info requests!
	if method != "/ranger.Node/Info" {
		n.rpcs = append(n.rpcs, req)
	}

	err := invoker(ctx, method, req, res, cc, opts...)
	log.Printf("RPC: method=%s; Error=%v", method, err)
	return err
}

func (n *TestNode) withSpyInterceptor() grpc.DialOption {
	return grpc.WithUnaryInterceptor(n.spyInterceptor)
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

	// Check that current state matches expected state.
	act := n.rglt.State(rID)
	if act != exp {
		t.Fatalf("can't advance to state %s when current state is %s (expected: %s)", new, act, exp)
		return
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
	n.transitions[ris] = err

	// Now wait until it's gone.
	// This assumes that some other goroutine is sitting in waitUntil.
	// TODO: Verify that before registering the transition. Fail the test if
	//       we try to advance while nobody is waiting.
	for {
		if _, ok := n.transitions[ris]; !ok {
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
