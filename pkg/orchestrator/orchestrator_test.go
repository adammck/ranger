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

	var err error
	ts.ks, err = keyspace.New(ts.cfg, pers)
	if err != nil {
		ts.T().Fatalf("keyspace.New: %s", err)
	}

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
		tickWait(ts.orch)
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

type Waiter interface {
	Wait()
}

// tickWait performs a Tick, then waits for any pending RPCs to complete, then
// waits for any give Waiters (which are probably fake_node.Barrier instances)
// to return.
//
// This allows us to pretend that Ticks will never begin while RPCs scheduled
// during the previous tick are still in flight, without sleeping or anything
// like that.
func tickWait(orch *Orchestrator, waiters ...Waiter) {
	orch.Tick()
	orch.WaitRPCs()

	for _, w := range waiters {
		w.Wait()
	}
}

func tickUntilStable(ts *OrchestratorSuite) {
	var ksPrev, rostPrev string
	var ticks, same int

	for {
		tickWait(ts.orch)
		ts.rost.Tick()

		ksNow := ts.ks.LogString()
		rostNow := ts.rost.TestString()

		// Changed since last time? Keep ticking.
		if ksPrev != ksNow || rostPrev != rostNow {
			same = 0
		} else {
			same += 1
		}

		ksPrev = ksNow
		rostPrev = rostNow

		// Stable for a few ticks? We're done.
		if same >= 2 {
			break
		}

		ticks += 1
		if ticks > 50 {
			ts.FailNow("didn't stablize after 50 ticks")
			return
		}
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

func (ts *OrchestratorSuite) TestPlacementFast() {
	initTestPlacement(ts)
	tickUntilStable(ts)

	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestPlacementMedium() {
	initTestPlacement(ts)

	// First tick: Placement created, Give RPC sent to node and returned
	// successfully. Remote state is updated in roster, but not keyspace.

	tickWait(ts.orch)
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

	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// Second tick: Keyspace is updated with state from roster. No RPCs sent.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// Third: Serve RPC is sent, to advance to ready. Returns success, and
	// roster is updated. Keyspace is not.

	tickWait(ts.orch)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// Forth: Keyspace is updated with ready state from roster. No RPCs sent.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// No more changes. This is steady state.

	ts.EnsureStable()
}

func (ts *OrchestratorSuite) TestPlacementSlow() {
	initTestPlacement(ts)
	ts.nodes.SetStrictTransitions(true)

	par := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsPreparing, nil)
	tickWait(ts.orch, par)
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

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	// The node finished preparing, but we don't know about it.
	par.Release()
	ts.Equal("{test-aaa [1:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	ar := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsReadying, nil)
	tickWait(ts.orch, ar)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ar.Release()
	ts.Equal("{test-aaa [1:NsReadying]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
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
	ts.nodes.SetStrictTransitions(false)

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

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", ts.ks.LogString())

	// Orchestrator advances to drop the placement, but (unlike when moving)
	// doesn't bother to notify the node via RPC. It has already told us that it
	// doesn't have the range.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", ts.ks.LogString())

	// The placement is destroyed.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())

	// From here we continue as usual. No need to repeat TestPlacement.

	tickWait(ts.orch)
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
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestMoveFast() {
	r1 := initTestMove(ts, false)
	ts.EnsureStable()

	func() {
		ts.orch.opMovesMu.Lock()
		defer ts.orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opMoves = append(ts.orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	tickUntilStable(ts)

	// Range moved from aaa to bbb.
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestMoveSlow() {
	r1 := initTestMove(ts, true)
	ts.EnsureStable()

	func() {
		ts.orch.opMovesMu.Lock()
		defer ts.orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		ts.orch.opMoves = append(ts.orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	bPAR := ts.nodes.Get("test-bbb").Expect(ts.T(), 1, state.NsPreparing, nil)
	tickWait(ts.orch, bPAR)
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
	bPAR.Release()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	aPDR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsTaking, nil)
	tickWait(ts.orch, aPDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	aPDR.Release() // Node A finished taking.
	ts.Equal("{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", ts.rost.TestString())

	bAR := ts.nodes.Get("test-bbb").Expect(ts.T(), 1, state.NsReadying, nil)
	tickWait(ts.orch, bAR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; ts.Len(bbb, 1) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 1}, bbb[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	bAR.Release() // Node B finished becoming ready.
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", ts.rost.TestString())

	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	aDR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsDropping, nil)
	tickWait(ts.orch, aDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	aDR.Release() // Node A finished dropping.
	ts.Equal("{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// test-aaa is gone!
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady:replacing(test-aaa)}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb [1:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()
}

func (ts *OrchestratorSuite) TestSplitFast() {
	r1 := initTestSplit(ts, false)
	ts.EnsureStable()

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	ts.orch.opSplitsMu.Lock()
	ts.orch.opSplits[r1.Meta.Ident] = op
	ts.orch.opSplitsMu.Unlock()

	tickUntilStable(ts)

	// Range 1 was split into ranges 2 and 3 at ccc.
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
}

func (ts *OrchestratorSuite) TestSplitSlow() {
	r1 := initTestSplit(ts, true)

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	ts.orch.opSplitsMu.Lock()
	ts.orch.opSplits[r1.Meta.Ident] = op
	ts.orch.opSplitsMu.Unlock()

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	// 2. Controller places new ranges on nodes.

	a2PAR := ts.nodes.Get("test-aaa").Expect(ts.T(), 2, state.NsPreparing, nil)
	a3PAR := ts.nodes.Get("test-aaa").Expect(ts.T(), 3, state.NsPreparing, nil)
	tickWait(ts.orch, a2PAR, a3PAR)
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

	a2PAR.Release() // R2 finished preparing, but R3 has not yet.
	ts.Equal("{test-aaa [1:NsReady, 2:NsPreparing, 3:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Gives
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	a3PAR.Release() // R3 becomes Prepared, too.
	ts.Equal("{test-aaa [1:NsReady, 2:NsPrepared, 3:NsPreparing]}", ts.rost.TestString())

	tickWait(ts.orch)
	// Note that we're not sending (redundant) Give RPCs to R2 any more.
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Give
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	// 4. Controller takes placements in parent range.

	a1PDR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsTaking, nil)
	tickWait(ts.orch, a1PDR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	a1PDR.Release() // r1p0 finishes taking.
	ts.Equal("{test-aaa [1:NsTaking, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Take
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsPrepared, 3:NsPrepared]}", ts.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	a2AR := ts.nodes.Get("test-aaa").Expect(ts.T(), 2, state.NsReadying, nil)
	a3AR := ts.nodes.Get("test-aaa").Expect(ts.T(), 3, state.NsReadying, nil)
	tickWait(ts.orch, a2AR, a3AR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 2) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 2}, aaa[0])
			ts.ProtoEqual(&pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	a3AR.Release() // r3p0 becomes ready.
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReadying]}", ts.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 2) // redundant Serves
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", ts.ks.LogString())

	a2AR.Release() // r2p0 becomes ready.
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReadying, 3:NsReady]}", ts.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Serve
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [1:NsTaken, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// 6. Orchestrator instructs parent range to drop placements.

	a1DR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsDropping, nil)
	tickWait(ts.orch, a1DR)
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			ts.ProtoEqual(&pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [1:NsDropping, 2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	a1DR.Release() // r1p0 finishes dropping.

	tickWait(ts.orch)
	ts.Len(RPCs(ts.nodes.RPCs()), 1) // redundant Drop
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())
	ts.Equal("{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Orchestrator doesn't notice this, but
	// maybe should, after sending the redundant Drop RPC?
	ts.rost.Tick()
	ts.Equal("{test-aaa [2:NsReady, 3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			ts.NoError(err)
		}
	default:
		ts.Fail("expected op.Err to be closed")
	}
}

func (ts *OrchestratorSuite) TestJoinFast() {
	r1, r2 := initTestJoin(ts, false)
	ts.EnsureStable()

	op := OpJoin{
		Left:  r1.Meta.Ident,
		Right: r2.Meta.Ident,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	ts.orch.opJoinsMu.Lock()
	ts.orch.opJoins = append(ts.orch.opJoins, op)
	ts.orch.opJoinsMu.Unlock()

	tickUntilStable(ts)

	// Ranges 1 and 2 were joined into range 3, which holds the entire keyspace.
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())
}

func (ts *OrchestratorSuite) TestJoinSlow() {
	r1, r2 := initTestJoin(ts, true)

	// Inject a join to get us started.
	// TODO: Do this via the operator interface instead.
	// TODO: Inject the target node for r3. It currently defaults to the empty
	//       node

	op := OpJoin{
		Left:  r1.Meta.Ident,
		Right: r2.Meta.Ident,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	ts.orch.opJoinsMu.Lock()
	ts.orch.opJoins = append(ts.orch.opJoins, op)
	ts.orch.opJoinsMu.Unlock()

	// 1. Controller initiates join.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	c3PAR := ts.nodes.Get("test-ccc").Expect(ts.T(), 3, state.NsPreparing, nil)
	tickWait(ts.orch, c3PAR)
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

	c3PAR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	a1PDR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsTaking, nil)
	b2PDR := ts.nodes.Get("test-bbb").Expect(ts.T(), 2, state.NsTaking, nil)
	tickWait(ts.orch, a1PDR, b2PDR)
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

	a1PDR.Release()
	b2PDR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", ts.rost.TestString())

	c3AR := ts.nodes.Get("test-ccc").Expect(ts.T(), 3, state.NsReadying, nil)
	tickWait(ts.orch)
	c3AR.Wait()
	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; ts.Len(ccc, 1) {
			ts.ProtoEqual(&pb.ServeRequest{Range: 3}, ccc[0])
		}
	}

	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", ts.rost.TestString())

	// 5. New range becomes ready.

	c3AR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", ts.rost.TestString())

	a1DR := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsDropping, nil)
	b2DR := ts.nodes.Get("test-bbb").Expect(ts.T(), 2, state.NsDropping, nil)
	tickWait(ts.orch, a1DR, b2DR)
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
	a1DR.Release()
	b2DR.Release()
	ts.rost.Tick()
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Equal("{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", ts.rost.TestString())

	ts.EnsureStable()

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			ts.NoError(err)
		}
	default:
		ts.Fail("expected op.Err to be closed")
	}
}

func (ts *OrchestratorSuite) TestSlowRPC() {

	// One node, one range, unplaced.

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
	ts.nodes.Get(na.Ident).SetGracePeriod(3 * time.Second)

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// -------------------------------------------------------------------------

	// No WaitRPCs() this time! But we do stil have to wait until our one
	// expected RPC reaches the barrier (via PrepareAddRange). Otherwise, Tick
	// without WaitRPCs will likely return before the RPC has even started, let
	// alone gotten to the barrier.
	par := ts.nodes.Get("test-aaa").Expect(ts.T(), 1, state.NsPreparing, nil)
	ts.orch.Tick()
	par.Wait()

	if rpcs := ts.nodes.RPCs(); ts.Equal([]string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; ts.Len(aaa, 1) {
			if ts.IsType(&pb.GiveRequest{}, aaa[0]) {
				ts.ProtoEqual(&pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				}, aaa[0].(*pb.GiveRequest).Range)
			}
		}
	}

	// Placement has been moved into PsPending, because we ticked past it...
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())

	// But the roster hasn't been updated yet, because the RPC hasn't completed.
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// RPC above is still in flight when the next Tick starts!
	// (This is quite unusual for tests.)
	ts.orch.Tick()

	// No redundant RPC this time.
	ts.Empty(ts.nodes.RPCs())

	// No state change; nothing happened.
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())

	// Give RPC finally completes! Roster is updated.
	par.Release()
	ts.orch.WaitRPCs()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsPrepared]}", ts.rost.TestString())

	// Subsequent ticks continue the placement as usual. No need to verify the
	// details in this test.
	tickUntilStable(ts)
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())
}

func TestOrchestratorSuite(t *testing.T) {
	suite.Run(t, new(OrchestratorSuite))
}

// -----------------------------------------------------------------------------

func initTestPlacement(ts *OrchestratorSuite) {
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

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive}", ts.ks.LogString())
	ts.Equal("{test-aaa []}", ts.rost.TestString())
}

// initTestMove spawns two hosts (aaa, bbb), one range (1), and one placement
// (range 1 is on aaa in PsReady), and returns the range.
func initTestMove(ts *OrchestratorSuite, strict bool) *ranje.Range {
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
	ts.nodes.SetStrictTransitions(strict)

	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb []}", ts.rost.TestString())

	return r1
}

func initTestSplit(ts *OrchestratorSuite, strict bool) *ranje.Range {

	// Nodes
	na := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	// Controller-side
	r1 := &ranje.Range{
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
	ts.Init([]*ranje.Range{r1})

	// Nodes-side
	ts.nodes.Add(ts.ctx, na, map[ranje.Ident]*info.RangeInfo{
		r1.Meta.Ident: {
			Meta:  r1.Meta,
			State: state.NsReady,
		},
	})
	ts.nodes.SetStrictTransitions(strict)

	// Probe the fake nodes.
	ts.rost.Tick()
	ts.Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]}", ts.rost.TestString())

	tickWait(ts.orch)
	ts.Empty(ts.nodes.RPCs())
	ts.Require().Equal("{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", ts.ks.LogString())

	return r1
}

// initTestJoin sets up three hosts (aaa, bbb, ccc), two ranges (1, 2) split at
// ggg, and two placements (r1 on aaa, r2 on bbb; both in PsReady), and returns
// the two ranges.
func initTestJoin(ts *OrchestratorSuite, strict bool) (*ranje.Range, *ranje.Range) {

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
	ts.nodes.SetStrictTransitions(strict)

	// Probe the fake nodes to verify the setup.

	ts.rost.Tick()
	ts.Equal("{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}", ts.ks.LogString())
	ts.Equal("{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", ts.rost.TestString())

	return r1, r2
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
