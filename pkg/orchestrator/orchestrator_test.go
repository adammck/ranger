package orchestrator

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"context"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
)

type OrchestratorSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *fake_nodes.TestNodes
	ks    *keyspace.Keyspace
	rost  *roster.Roster
	orch  *Orchestrator
}

// ProtoEqual is a helper to compare two slices of protobufs. It's not great.
func (ts *OrchestratorSuite) ProtoEqual(expected, actual interface{}) {
	if diff := cmp.Diff(expected, actual, protocmp.Transform()); diff != "" {
		ts.Fail(fmt.Sprintf("Not equal (-want +got):\n%s\n", diff))
	}
}

// nIDs is a helper to extract the list of nIDs from map like RPCs returns.
func nIDs(obj map[string][]interface{}) []string {
	ret := []string{}

	for k := range obj {
		ret = append(ret, k)
	}

	sort.Strings(ret)
	return ret
}

func RPCs(obj map[string][]interface{}) []interface{} {
	ret := []interface{}{}

	for _, v := range obj {
		ret = append(ret, v...)
	}

	return ret
}

// GetRange returns a range from the test's keyspace or fails the test.
func (ts *OrchestratorSuite) GetRange(rID uint64) *ranje.Range {
	r, err := ts.ks.Get(ranje.Ident(rID))
	ts.Require().NoError(err)
	return r
}

// Init should be called at the top of each test to define the state of the
// world. Nothing will work until this method is called.
func (ts *OrchestratorSuite) Init(ranges []*ranje.Range) {
	pers := &FakePersister{ranges: ranges}
	ts.ks = keyspace.New(ts.cfg, pers)
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	ts.orch = New(ts.cfg, ts.ks, ts.rost, srv)
}

func (ts *OrchestratorSuite) SetupTest() {
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
}

func (ts *OrchestratorSuite) EnsureStable() {
	ksLog := ts.ks.LogString()
	rostLog := ts.rost.TestString()
	for i := 0; i < 2; i++ {
		ts.orch.Tick()
		ts.rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		ts.Require().Equal(ksLog, ts.ks.LogString())
		ts.Require().Equal(rostLog, ts.rost.TestString())
	}
}

func (ts *OrchestratorSuite) TearDownTest() {
	if ts.nodes != nil {
		ts.nodes.Close()
	}
}

func (ts *OrchestratorSuite) TestJunk() {
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

func (ts *OrchestratorSuite) TestPlacement() {

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

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				},
				// TODO: It's weird and kind of useless for this to be in here.
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Parent: []uint64{},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())
	// TODO: Assert that new placement was persisted

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	// The node finished preparing, but we don't know about it.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsPrepared, nil)
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsReady, nil)
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *OrchestratorSuite) TestMissingPlacement() {

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

	// Orchestrator notices that the node doesn't have the range, so marks the
	// placement as abandoned.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", ts.ks.LogString())

	// Orchestrator advances to drop the placement, but (unlike when moving) doesn't
	// bother to notify the node via RPC. It has already told us that it doesn't
	// have the range.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", ts.ks.LogString())

	// The placement is destroyed.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())

	// From here we continue as usual. No need to repeat TestPlacement.

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestMove() {

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
		ts.orch.opMovesMu.Lock()
		defer ts.orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opMoves = append(ts.orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			//Node:  "test-aaa",
			Dest: "test-bbb",
		})
	}()

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
							{
								Node:  "host-bbb:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPending:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	// Node B finished preparing.
	ts.nodes.Get("test-bbb").AdvanceTo(ts.T(), 1, state.NsPrepared, nil)
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Node A finished taking.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsTaken, nil)
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 1}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	// Node B finished becoming ready.
	ts.nodes.Get("test-bbb").AdvanceTo(ts.T(), 1, state.NsReady, nil)
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsNotFound, nil)

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// IsReplacing annotation is gone.
	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *OrchestratorSuite) TestSplit() {

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

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Require().Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// -------------------------------------------------------------------------

	func() {
		ts.orch.opSplitsMu.Lock()
		defer ts.orch.opSplitsMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opSplits[r0.Meta.Ident] = OpSplit{
			Range: r0.Meta.Ident,
			Key:   "ccc",
		}
	}()

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	// 2. Controller places new ranges on nodes.

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 2) {
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 2,
					Start: []byte(ranje.ZeroKey),
					End:   []byte("ccc"),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 2,
							Start: []byte(ranje.ZeroKey),
							End:   []byte("ccc"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[0])
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 3,
					Start: []byte("ccc"),
					End:   []byte(ranje.ZeroKey),
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 3,
							Start: []byte("ccc"),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							Start: []byte(ranje.ZeroKey),
							End:   []byte(ranje.ZeroKey),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[1])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	// 3. Wait for placements to become Prepared.

	// R2 finished preparing, but R3 has not yet.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 2, state.NsPrepared, nil)
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Gives
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// R3 becomes Prepared, too.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 3, state.NsPrepared, nil)
	ts.Equal("{test-aaa [1:NsReady, 2:NsPrepared, 3:NsPreparing]}", ts.rost.TestString())

	ts.orch.Tick()
	// Note that we're not sending (redundant) Give RPCs to R2 any more.
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// 4. Controller takes placements in parent range.

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// r1p0 finishes taking.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsTaken, nil)
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 2) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 2}, aaa[0])
			ts.ProtoEqual(&pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	// r3p0 becomes ready.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 3, state.NsReady, nil)

	// Orchestrator notices on next tick.
	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Serves
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// r2p0 becomes ready.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 2, state.NsReady, nil)

	// Orchestrator notices on next tick.
	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// 6. Orchestrator instructs parent range to drop placements.

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// r1p0 finishes dropping.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsNotFound, nil)

	ts.orch.Tick()
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Orchestrator doesn't notice this, but
	// maybe should, after sending the redundant Drop RPC?
	ts.rost.Tick()
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *OrchestratorSuite) TestJoin() {

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
		ts.orch.opJoinsMu.Lock()
		defer ts.orch.opJoinsMu.Unlock()

		ts.orch.opJoins = append(ts.orch.opJoins, OpJoin{
			Left:  r1.Meta.Ident,
			Right: r2.Meta.Ident,
		})
	}()

	// 1. Controller initiates join.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; ts.Len(ccc, 1) {
			ts.ProtoEqual(&pb.GiveRequest{
				Range: &pb.RangeMeta{
					Ident: 3,
				},
				Parents: []*pb.Parent{
					{
						Range: &pb.RangeMeta{
							Ident: 3,
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-ccc:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 1,
							End:   []byte("ggg"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
					{
						Range: &pb.RangeMeta{
							Ident: 2,
							Start: []byte("ggg"),
						},
						Placements: []*pb.Placement{
							{
								Node:  "host-bbb:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, ccc[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPreparing]}", ts.rost.TestString())

	// 2. New range finishes preparing.

	ts.nodes.Get("test-ccc").AdvanceTo(ts.T(), 3, state.NsPrepared, nil)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 2}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [2:NsTaking]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 4. Old ranges becomes taken.

	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsTaken, nil)
	ts.nodes.Get("test-bbb").AdvanceTo(ts.T(), 2, state.NsTaken, nil)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; ts.Len(ccc, 1) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 3}, ccc[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", ts.rost.TestString())

	// 5. New range becomes ready.

	ts.nodes.Get("test-ccc").AdvanceTo(ts.T(), 3, state.NsReady, nil)
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 2}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	// Drops finish.
	ts.nodes.Get("test-aaa").AdvanceTo(ts.T(), 1, state.NsNotFound, nil)
	ts.nodes.Get("test-bbb").AdvanceTo(ts.T(), 2, state.NsNotFound, nil)
	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.orch.Tick()
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(OrchestratorSuite))
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
