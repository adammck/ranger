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

// Init should be called at the top of each test to define the state of the
// world. Nothing will work until this method is called.
func (ts *BalancerSuite) Init(ranges []*ranje.Range) {
	pers := &FakePersister{ranges: ranges}
	ts.ks = ranje.New(ts.cfg, pers)
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	ts.bal = New(ts.cfg, ts.ks, ts.rost, srv)
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
	ts.rost = roster.New(ts.cfg, ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory

	// TODO: Get this stupid RpcSpy out of the actual code and into this file,
	//       maybe call back from the handlers in TestNode.
	ts.spy = NewRpcSpy()
	ts.rost.RpcSpy = ts.spy.c
	ts.spy.Start()

}

func (ts *BalancerSuite) EnsureStable() {
	ksLog := ts.ks.LogString()
	rostLog := ts.rost.TestString()
	for i := 0; i < 2; i++ {
		ts.bal.Tick()
		ts.rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		//ts.Require().Zero(len(ts.spy.Get()))
		ts.Require().Equal(ksLog, ts.ks.LogString())
		ts.Require().Equal(rostLog, ts.rost.TestString())
	}
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
	ts.Init(nil)

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
	ts.Init(nil)

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}, nil)

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
	// TODO: Refactor to initiate move via OpMove

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}, nil)

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	{
		r := ts.GetRange(1)
		p := r.Placements[0]
		p.SetWantMoveTo(ranje.AnyNode())
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move(any)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move(any) p1=test-bbb:PsPending}", ts.ks.LogString())
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
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move(any) p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move(any) p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady:want-move(any) p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Node A finished taking.
	ts.nodes.RangeState("test-aaa", 1, roster.NsTaken)
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move(any) p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move(any) p1=test-bbb:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	// Node B finished becoming ready.
	ts.nodes.RangeState("test-bbb", 1, roster.NsReady)
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move(any) p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move(any) p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.nodes.FinishDrop(ts, "test-aaa", 1)

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken:want-move(any) p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped:want-move(any) p1=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *BalancerSuite) TestSplit() {

	// Nodes
	na := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	// Controller-side
	r0 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
		},
		State: ranje.RsActive,
		// TODO: Test that controller-side placements are repaired when a node
		//       shows up claiming to have a (valid) placement.
		Placements: []*ranje.Placement{{
			NodeID: na.Ident,
			State:  ranje.PsReady,
		}},
	}
	ts.Init([]*ranje.Range{r0})

	// Nodes-side
	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*roster.RangeInfo{
		r0.Meta.Ident: {
			Meta:  r0.Meta,
			State: roster.NsReady,
		},
	})

	// Probe the fake nodes.
	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Require().Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// -------------------------------------------------------------------------

	func() {
		ts.bal.opSplitsMu.Lock()
		defer ts.bal.opSplitsMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.bal.opSplits[r0.Meta.Ident] = OpSplit{
			Range: r0.Meta.Ident,
			Key:   "ccc",
		}
	}()

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	// 2. Controller places new ranges on nodes.

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Give, Node: "test-aaa", Range: 2},
		{Type: roster.Give, Node: "test-aaa", Range: 3},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	// 3. Wait for placements to become Prepared.

	// R2 finished preparing, but R3 has not yet.
	ts.nodes.RangeState("test-aaa", 2, roster.NsPrepared)
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Give, Node: "test-aaa", Range: 2}, // redundant
		{Type: roster.Give, Node: "test-aaa", Range: 3}, // redundant
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// R3 becomes Prepared, too.
	ts.nodes.RangeState("test-aaa", 3, roster.NsPrepared)
	ts.Equal("{test-aaa [1:NsReady, 2:NsPrepared, 3:NsPreparing]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		// Note that we're not sending (redundant) Give RPCs to R2 any more.
		{Type: roster.Give, Node: "test-aaa", Range: 3}, // redundant
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// 4. Controller takes placements in parent range.

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Take, Node: "test-aaa", Range: 1},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// r1p0 finishes taking.
	ts.nodes.RangeState("test-aaa", 1, roster.NsTaken)
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Take, Node: "test-aaa", Range: 1}, // redundant
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-aaa", Range: 2},
		{Type: roster.Serve, Node: "test-aaa", Range: 3},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	// r3p0 becomes ready.
	ts.nodes.RangeState("test-aaa", 3, roster.NsReady)

	// Balancer notices on next tick.
	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-aaa", Range: 2}, // redundant
		{Type: roster.Serve, Node: "test-aaa", Range: 3}, // redundant
	}, ts.spy.Get())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// r2p0 becomes ready.
	ts.nodes.RangeState("test-aaa", 2, roster.NsReady)

	// Balancer notices on next tick.
	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-aaa", Range: 2}, // redundant
	}, ts.spy.Get())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// 6. Balancer instructs parent range to drop placements.

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Drop, Node: "test-aaa", Range: 1},
	}, ts.spy.Get())
	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Drop, Node: "test-aaa", Range: 1}, // redundant
	}, ts.spy.Get())
	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// r1p0 finishes dropping.
	ts.nodes.FinishDrop(ts, "test-aaa", 1)

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Drop, Node: "test-aaa", Range: 1}, // redundant
	}, ts.spy.Get())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Balancer doesn't notice this, but
	// maybe should, after sending the redundant Drop RPC?
	ts.rost.Tick()
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *BalancerSuite) TestJoin() {

	// Start with two ranges (which togeter cover the whole keyspace) assigned
	// to two of three nodes. The ranges will be joined onto the third node.

	na := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}
	nb := discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}
	nc := discovery.Remote{
		Ident: "test-ccc",
		Host:  "host-ccc",
		Port:  1,
	}

	r1 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
			End:   ranje.Key("ggg"),
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: na.Ident,
			State:  ranje.PsReady,
		}},
	}
	r2 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 2,
			Start: ranje.Key("ggg"),
			End:   ranje.ZeroKey,
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: nb.Ident,
			State:  ranje.PsReady,
		}},
	}
	ts.Init([]*ranje.Range{r1, r2})

	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*roster.RangeInfo{
		r1.Meta.Ident: {
			Meta:  r1.Meta,
			State: roster.NsReady,
		},
	})
	ts.nodes.Add(ts.ctx, nb, map[ranje.Ident]*roster.RangeInfo{
		r2.Meta.Ident: {
			Meta:  r2.Meta,
			State: roster.NsReady,
		},
	})
	ts.nodes.Add(ts.ctx, nc, map[ranje.Ident]*roster.RangeInfo{})

	// Probe the fake nodes to verify the setup.

	ts.rost.Tick()
	ts.Equal("{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	ts.EnsureStable()

	// -------------------------------------------------------------------------

	// Inject a join to get us started.
	// TODO: Do this via the operator interface instead.
	// TODO: Inject the target node for r3. It currently defaults to the empty
	//       node

	func() {
		ts.bal.opJoinsMu.Lock()
		defer ts.bal.opJoinsMu.Unlock()

		ts.bal.opJoins = append(ts.bal.opJoins, OpJoin{
			Left:  r1.Meta.Ident,
			Right: r2.Meta.Ident,
		})
	}()

	// 1. Controller initiates join.

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Give, Node: "test-ccc", Range: 3},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPreparing]}", ts.rost.TestString())

	// 2. New range finishes preparing.

	ts.nodes.RangeState("test-ccc", 3, roster.NsPrepared)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Take, Node: "test-aaa", Range: 1},
		{Type: roster.Take, Node: "test-bbb", Range: 2},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [2:NsTaking]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 4. Old ranges becomes taken.

	ts.nodes.RangeState("test-aaa", 1, roster.NsTaken)
	ts.nodes.RangeState("test-bbb", 2, roster.NsTaken)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-ccc", Range: 3},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", ts.rost.TestString())

	// 5. New range becomes ready.

	ts.nodes.RangeState("test-ccc", 3, roster.NsReady)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Drop, Node: "test-aaa", Range: 1},
		{Type: roster.Drop, Node: "test-bbb", Range: 2},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	// Drops finish.
	ts.nodes.FinishDrop(ts, "test-aaa", 1)
	ts.nodes.FinishDrop(ts, "test-bbb", 2)
	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
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

type FakePersister struct {
	ranges []*ranje.Range
	called bool
}

func (fp *FakePersister) GetRanges() ([]*ranje.Range, error) {
	if fp.called {
		// PutRanges is not implemented, so this doesn't make sense.
		panic("FakePersister.GetRanges called more than once")
	}
	fp.called = true

	if fp.ranges != nil {
		return fp.ranges, nil
	}

	return []*ranje.Range{}, nil
}

func (fp *FakePersister) PutRanges([]*ranje.Range) error {
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

func NewTestNode(rangeInfos map[ranje.Ident]*roster.RangeInfo) *TestNode {
	ranges := map[ranje.Ident]*testRange{}
	//if rangeInfos != nil
	for rID, ri := range rangeInfos {
		ranges[rID] = &testRange{info: ri}
	}

	return &TestNode{
		ranges: ranges,
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
		log.Printf("got redundant Drop (no such range; maybe drop complete)")

		// This is NOT a failure.
		return &pb.DropResponse{
			State: roster.NsNotFound.ToProto(),
		}, nil
	}

	switch r.info.State {
	case roster.NsTaken:
		// Actual state transition. We don't actually drop anything here, only
		// claim that we are doing so, to simulate a slow client. Test must call
		// FinishDrop to move to NsDropped.
		r.info.State = roster.NsDropping

	case roster.NsDropping:
		log.Printf("got redundant Drop (drop in progress)")

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

func (tn *TestNodes) Add(ctx context.Context, remote discovery.Remote, rangeInfos map[ranje.Ident]*roster.RangeInfo) {
	n := NewTestNode(rangeInfos)
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
