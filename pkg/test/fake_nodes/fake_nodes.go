package fake_nodes

import (
	"fmt"

	"context"

	"github.com/adammck/ranger/pkg/discovery"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/test/fake_node"
	"google.golang.org/grpc"
)

type TestNodes struct {
	disc    *mockdisc.MockDiscovery
	nodes   map[string]*fake_node.TestNode // nID
	closers []func()
}

func NewTestNodes() *TestNodes {
	tn := &TestNodes{
		disc:  mockdisc.New(),
		nodes: map[string]*fake_node.TestNode{},
	}

	return tn
}

func (tn *TestNodes) Close() {
	for _, f := range tn.closers {
		f()
	}
}

func (tn *TestNodes) Add(ctx context.Context, remote discovery.Remote, rangeInfos map[ranje.Ident]*info.RangeInfo) {
	n, closer := fake_node.NewTestNode(ctx, remote.Addr(), rangeInfos)
	tn.nodes[remote.Ident] = n
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

// Use this to stub out the Roster.
func (tn *TestNodes) NodeConnFactory(ctx context.Context, remote discovery.Remote) (*grpc.ClientConn, error) {
	for _, n := range tn.nodes {
		if n.Addr == remote.Addr() {
			return n.Conn, nil
		}
	}

	return nil, fmt.Errorf("no such connection: %v", remote.Addr())
}

func (tn *TestNodes) Discovery() *mockdisc.MockDiscovery {
	return tn.disc
}
