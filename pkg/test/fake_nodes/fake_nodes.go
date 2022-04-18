package fake_nodes

import (
	"fmt"
	"log"
	"testing"

	"context"

	"github.com/adammck/ranger/pkg/discovery"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_node"
	"google.golang.org/grpc"
)

type TestNodes struct {
	disc    *mockdisc.MockDiscovery
	nodes   map[string]*fake_node.TestNode // nID
	conns   map[string]*grpc.ClientConn    // addr
	closers []func()
}

func NewTestNodes() *TestNodes {
	tn := &TestNodes{
		disc:  mockdisc.New(),
		nodes: map[string]*fake_node.TestNode{},
		conns: map[string]*grpc.ClientConn{},
	}

	return tn
}

func (tn *TestNodes) Close() {
	for _, f := range tn.closers {
		f()
	}
}

func (tn *TestNodes) Add(ctx context.Context, remote discovery.Remote, rangeInfos map[ranje.Ident]*info.RangeInfo) {

	n := fake_node.NewTestNode(rangeInfos)
	tn.nodes[remote.Ident] = n

	conn, closer := nodeServer(ctx, n)
	tn.conns[remote.Addr()] = conn
	tn.closers = append(tn.closers, closer)

	tn.disc.Add("node", remote)
}

func (tn *TestNodes) Get(nID string) *fake_node.TestNode {
	n, ok := tn.nodes[nID]
	if !ok {
		panic(fmt.Sprintf("no such node: %s", nID))
	}

	return n
}

// RPCs returns a map of NodeID to the (protos) requests which have been
// received by any node since the last time this method was called.
func (tn *TestNodes) RPCs() map[string][]interface{} {
	ret := map[string][]interface{}{}

	for nID, n := range tn.nodes {
		if rpcs := n.RPCs(); len(rpcs) > 0 {
			ret[nID] = rpcs
		}
	}

	return ret
}

func (tn *TestNodes) RangeState(nID string, rID ranje.Ident, state state.RemoteState) {
	n, ok := tn.nodes[nID]
	if !ok {
		panic(fmt.Sprintf("no such node: %s", nID))
	}

	r, ok := n.TestRanges[rID]
	if !ok {
		panic(fmt.Sprintf("no such range: %s", rID))
	}

	log.Printf("RangeState: %s, %s -> %s", nID, rID, state)
	r.Info.State = state
}

func (tn *TestNodes) FinishDrop(t *testing.T, nID string, rID ranje.Ident) {
	n, ok := tn.nodes[nID]
	if !ok {
		t.Fatalf("no such node: %s", nID)
	}

	r, ok := n.TestRanges[rID]
	if !ok {
		t.Fatalf("can't drop unknown range: %s", rID)
		return
	}

	if r.Info.State != state.NsDropping {
		t.Fatalf("can't drop range not in NsDropping: rID=%s, state=%s", rID, r.Info.State)
		return
	}

	log.Printf("FinishDrop: nID=%s, rID=%s", nID, rID)
	delete(n.TestRanges, rID)
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
