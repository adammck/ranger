package balancer

import (
	"sync"
	"testing"
	"time"

	"context"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type BalancerSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *fake_nodes.TestNodes
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

	ts.nodes = fake_nodes.NewTestNodes()
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

	na := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	r1 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
		},
		State: ranje.RsActive,
	}

	ts.Init([]*ranje.Range{r1})
	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{})

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
	ts.nodes.RangeState("test-aaa", 1, state.NsPrepared)
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
	ts.nodes.RangeState("test-aaa", 1, state.NsReady)
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *BalancerSuite) TestMissingPlacement() {

	// One node claiming that it's empty.
	na := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}
	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{})

	// One range, which the controller thinks should be on that node.
	r1 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: na.Ident, // <--
			State:  ranje.PsReady,
		}},
	}
	ts.Init([]*ranje.Range{r1})

	// Probe to verify initial state.

	ts.rost.Tick()
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// -------------------------------------------------------------------------

	// Balancer notices that the node doesn't have the range, so marks the
	// placement as abandoned.

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", ts.ks.LogString())

	// Balancer advances to drop the placement, but (unlike when moving) doesn't
	// bother to notify the node via RPC. It has already told us that it doesn't
	// have the range.

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", ts.ks.LogString())

	// The placement is destroyed.

	ts.bal.Tick()
	ts.Empty(ts.spy.Get())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())

	// From here we continue as usual. No need to repeat TestPlacement.

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())
}

func (ts *BalancerSuite) TestMove() {

	// Start with one range placed on node aaa, and an empty node bbb.

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

	r1 := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: na.Ident,
			State:  ranje.PsReady,
		}},
	}
	ts.Init([]*ranje.Range{r1})

	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{
		r1.Meta.Ident: {
			Meta:  r1.Meta,
			State: state.NsReady,
		},
	})
	ts.nodes.Add(ts.ctx, nb, map[ranje.Ident]*info.RangeInfo{})

	// Probe to verify initial state.

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	ts.EnsureStable()

	// -------------------------------------------------------------------------

	func() {
		ts.bal.opMovesMu.Lock()
		defer ts.bal.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.bal.opMoves = append(ts.bal.opMoves, OpMove{
			Range: r1.Meta.Ident,
			//Node:  "test-aaa",
			Dest: "test-bbb",
		})
	}()

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Give, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPending:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	// Node B finished preparing.
	ts.nodes.RangeState("test-bbb", 1, state.NsPrepared)
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get())) // TODO: Replace all these with: ts.Empty(ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Take, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Node A finished taking.
	ts.nodes.RangeState("test-aaa", 1, state.NsTaken)
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Serve, Node: "test-bbb", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	// Node B finished becoming ready.
	ts.nodes.RangeState("test-bbb", 1, state.NsReady)
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.nodes.FinishDrop(ts.T(), "test-aaa", 1)

	ts.bal.Tick()
	ts.Equal([]roster.RpcRecord{{Type: roster.Drop, Node: "test-aaa", Range: 1}}, ts.spy.Get()) // redundant
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// IsReplacing annotation is gone.
	ts.bal.Tick()
	ts.Equal(0, len(ts.spy.Get()))
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
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
	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{
		r0.Meta.Ident: {
			Meta:  r0.Meta,
			State: state.NsReady,
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
	ts.nodes.RangeState("test-aaa", 2, state.NsPrepared)
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Give, Node: "test-aaa", Range: 2}, // redundant
		{Type: roster.Give, Node: "test-aaa", Range: 3}, // redundant
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// R3 becomes Prepared, too.
	ts.nodes.RangeState("test-aaa", 3, state.NsPrepared)
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
	ts.nodes.RangeState("test-aaa", 1, state.NsTaken)
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
	ts.nodes.RangeState("test-aaa", 3, state.NsReady)

	// Balancer notices on next tick.
	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-aaa", Range: 2}, // redundant
		{Type: roster.Serve, Node: "test-aaa", Range: 3}, // redundant
	}, ts.spy.Get())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// r2p0 becomes ready.
	ts.nodes.RangeState("test-aaa", 2, state.NsReady)

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
	ts.nodes.FinishDrop(ts.T(), "test-aaa", 1)

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

	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{
		r1.Meta.Ident: {
			Meta:  r1.Meta,
			State: state.NsReady,
		},
	})
	ts.nodes.Add(ts.ctx, nb, map[ranje.Ident]*info.RangeInfo{
		r2.Meta.Ident: {
			Meta:  r2.Meta,
			State: state.NsReady,
		},
	})
	ts.nodes.Add(ts.ctx, nc, map[ranje.Ident]*info.RangeInfo{})

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

	ts.nodes.RangeState("test-ccc", 3, state.NsPrepared)
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

	ts.nodes.RangeState("test-aaa", 1, state.NsTaken)
	ts.nodes.RangeState("test-bbb", 2, state.NsTaken)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	ts.bal.Tick()
	ts.ElementsMatch([]roster.RpcRecord{
		{Type: roster.Serve, Node: "test-ccc", Range: 3},
	}, ts.spy.Get())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", ts.rost.TestString())

	// 5. New range becomes ready.

	ts.nodes.RangeState("test-ccc", 3, state.NsReady)
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
	ts.nodes.FinishDrop(ts.T(), "test-aaa", 1)
	ts.nodes.FinishDrop(ts.T(), "test-bbb", 2)
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
