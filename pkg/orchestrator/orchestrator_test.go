package orchestrator

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
)

// TODO: Move to keyspace tests.
func TestJunk(t *testing.T) {
	ksStr := ""
	rosStr := ""
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()

	orch.rost.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// ----

	r := mustGetRange(t, orch.ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	assert.Equal(t, ranje.RsActive, r.State, "range should be born active")
	assert.Equal(t, 0, len(r.Placements), "range should be born with no placements")
}

func TestPlacementFast(t *testing.T) {
	orch, nodes := initTestPlacement(t, false)
	defer nodes.Close()

	tickUntilStable(t, orch, nodes)

	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())
}

func TestPlacementReadyError(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()

	// Node aaa will always fail to become ready.
	nodes.Get("test-aaa").SetReturnValue(t, 1, state.NsReadying, fmt.Errorf("can't get ready!"))

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// ----

	tickUntilStable(t, orch, nodes)

	// TODO: This is not a good state! The controller has discarded the
	//       placement on aaa, but the node will remember it forever. That's the
	//       only way that it ended up on bbb, which I think is also buggy.
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReadyingError]} {test-bbb [1:NsReady]}", orch.rost.TestString())
}

func TestPlacementWithFailures(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()

	// Node aaa will always reject Gives.
	nodes.Get("test-aaa").SetReturnValue(t, 1, state.NsPreparing, fmt.Errorf("something went wrong"))

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// ----

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())
}

func TestPlacementMedium(t *testing.T) {
	orch, nodes := initTestPlacement(t, false)
	defer nodes.Close()

	// First tick: Placement created, Give RPC sent to node and returned
	// successfully. Remote state is updated in roster, but not keyspace.

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	// Second tick: Keyspace is updated with state from roster. No RPCs sent.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())

	// Third: Serve RPC is sent, to advance to ready. Returns success, and
	// roster is updated. Keyspace is not.

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())

	// Forth: Keyspace is updated with ready state from roster. No RPCs sent.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	// No more changes. This is steady state.

	requireStable(t, orch)
}

func TestPlacementSlow(t *testing.T) {
	orch, nodes := initTestPlacement(t, true)
	defer nodes.Close()

	par := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsPreparing)
	tickWait(orch, par)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", orch.rost.TestString())
	// TODO: Assert that new placement was persisted

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", orch.rost.TestString())

	// The node finished preparing, but we don't know about it.
	par.Release()
	assert.Equal(t, "{test-aaa [1:NsPreparing]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())

	ar := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsReadying)
	tickWait(orch, ar)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReadying]}", orch.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ar.Release()
	assert.Equal(t, "{test-aaa [1:NsReadying]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestMissingPlacement(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// ----

	// Orchestrator notices that the node doesn't have the range, so marks the
	// placement as abandoned.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// Orchestrator advances to drop the placement, but (unlike when moving)
	// doesn't bother to notify the node via RPC. It has already told us that it
	// doesn't have the range.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// The placement is destroyed.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// From here we continue as usual. No need to repeat TestPlacement.

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())
}

func TestMoveFast(t *testing.T) {
	orch, nodes, r1 := initTestMove(t, false)
	defer nodes.Close()

	requireStable(t, orch)

	func() {
		orch.opMovesMu.Lock()
		defer orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		orch.opMoves = append(orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	tickUntilStable(t, orch, nodes)

	// Range moved from aaa to bbb.
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", orch.ks.LogString())
}

func TestMoveSlow(t *testing.T) {
	orch, nodes, r1 := initTestMove(t, true)
	defer nodes.Close()

	requireStable(t, orch)

	func() {
		orch.opMovesMu.Lock()
		defer orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		orch.opMoves = append(orch.opMoves, OpMove{
			Range: r1.Meta.Ident,
			Dest:  "test-bbb",
		})
	}()

	bPAR := nodes.Get("test-bbb").AddBarrier(t, 1, state.NsPreparing)
	tickWait(orch, bPAR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
							{
								Node:  "host-test-bbb:1",
								State: pb.PlacementState_PS_PENDING,
							},
						},
					},
				},
			}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", orch.rost.TestString())

	// Node B finished preparing.
	bPAR.Release()
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPreparing]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	aPDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsTaking)
	tickWait(orch, aPDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Take
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady p1=test-bbb:PsPrepared:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	aPDR.Release() // Node A finished taking.
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsPrepared]}", orch.rost.TestString())

	bAR := nodes.Get("test-bbb").AddBarrier(t, 1, state.NsReadying)
	tickWait(orch, bAR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 1}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsPrepared:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", orch.rost.TestString())

	bAR.Release() // Node B finished becoming ready.
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReadying]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [1:NsReady]}", orch.rost.TestString())

	aDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDropping)
	tickWait(orch, aDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", orch.rost.TestString())

	aDR.Release() // Node A finished dropping.
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsTaken p1=test-bbb:PsReady:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsReady:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())

	// test-aaa is gone!
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsReady]}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestSplitFast(t *testing.T) {
	orch, nodes, r1 := initTestSplit(t, false)
	defer nodes.Close()

	requireStable(t, orch)

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	orch.opSplitsMu.Lock()
	orch.opSplits[r1.Meta.Ident] = op
	orch.opSplitsMu.Unlock()

	tickUntilStable(t, orch, nodes)

	// Range 1 was split into ranges 2 and 3 at ccc.
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())
}

func TestSplitSlow(t *testing.T) {
	orch, nodes, r1 := initTestSplit(t, true)
	defer nodes.Close()

	op := OpSplit{
		Range: r1.Meta.Ident,
		Key:   "ccc",
		Err:   make(chan error),
	}

	orch.opSplitsMu.Lock()
	orch.opSplits[r1.Meta.Ident] = op
	orch.opSplitsMu.Unlock()

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())

	// 2. Controller places new ranges on nodes.

	a2PAR := nodes.Get("test-aaa").AddBarrier(t, 2, state.NsPreparing)
	a3PAR := nodes.Get("test-aaa").AddBarrier(t, 3, state.NsPreparing)
	tickWait(orch, a2PAR, a3PAR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 2) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[0])
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-aaa:1",
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
								Node:  "host-test-aaa:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady 2:NsPreparing 3:NsPreparing]}", orch.rost.TestString())

	// 3. Wait for placements to become Prepared.

	a2PAR.Release() // R2 finished preparing, but R3 has not yet.
	assert.Equal(t, "{test-aaa [1:NsReady 2:NsPreparing 3:NsPreparing]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 2) // redundant Gives
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	a3PAR.Release() // R3 becomes Prepared, too.
	assert.Equal(t, "{test-aaa [1:NsReady 2:NsPrepared 3:NsPreparing]}", orch.rost.TestString())

	tickWait(orch)
	// Note that we're not sending (redundant) Give RPCs to R2 any more.
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())

	// 4. Controller takes placements in parent range.

	a1PDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsTaking)
	tickWait(orch, a1PDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking 2:NsPrepared 3:NsPrepared]}", orch.rost.TestString())

	a1PDR.Release() // r1p0 finishes taking.
	assert.Equal(t, "{test-aaa [1:NsTaking 2:NsPrepared 3:NsPrepared]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Take
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsReady} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsPrepared 3:NsPrepared]}", orch.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	a2AR := nodes.Get("test-aaa").AddBarrier(t, 2, state.NsReadying)
	a3AR := nodes.Get("test-aaa").AddBarrier(t, 3, state.NsReadying)
	tickWait(orch, a2AR, a3AR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 2) {
			ProtoEqual(t, &pb.ServeRequest{Range: 2}, aaa[0])
			ProtoEqual(t, &pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReadying 3:NsReadying]}", orch.rost.TestString())

	a3AR.Release() // r3p0 becomes ready.
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReadying 3:NsReadying]}", orch.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 2) // redundant Serves
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReadying 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsPrepared}", orch.ks.LogString())

	a2AR.Release() // r2p0 becomes ready.
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReadying 3:NsReady]}", orch.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsPrepared} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsTaken 2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	// 6. Orchestrator instructs parent range to drop placements.

	a1DR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDropping)
	tickWait(orch, a1DR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{test-aaa [1:NsDropping 2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{test-aaa [1:NsDropping 2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	a1DR.Release() // r1p0 finishes dropping.

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsTaken} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsReady} {3 (ccc, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Orchestrator doesn't notice this, but
	// maybe should, after sending the redundant Drop RPC?
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [2:NsReady 3:NsReady]}", orch.rost.TestString())

	requireStable(t, orch)

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			assert.NoError(t, err)
		}
	default:
		assert.Fail(t, "expected op.Err to be closed")
	}
}

func TestJoinFast(t *testing.T) {
	orch, nodes, r1, r2 := initTestJoin(t, false)
	defer nodes.Close()

	requireStable(t, orch)

	op := OpJoin{
		Left:  r1.Meta.Ident,
		Right: r2.Meta.Ident,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	orch.opJoinsMu.Lock()
	orch.opJoins = append(orch.opJoins, op)
	orch.opJoinsMu.Unlock()

	tickUntilStable(t, orch, nodes)

	// Ranges 1 and 2 were joined into range 3, which holds the entire keyspace.
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", orch.rost.TestString())
}

func TestJoinSlow(t *testing.T) {
	orch, nodes, r1, r2 := initTestJoin(t, true)
	defer nodes.Close()

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

	orch.opJoinsMu.Lock()
	orch.opJoins = append(orch.opJoins, op)
	orch.opJoinsMu.Unlock()

	// 1. Controller initiates join.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}", orch.rost.TestString())

	c3PAR := nodes.Get("test-ccc").AddBarrier(t, 3, state.NsPreparing)
	tickWait(orch, c3PAR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; assert.Len(t, ccc, 1) {
			ProtoEqual(t, &pb.GiveRequest{
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
								Node:  "host-test-ccc:1",
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
								Node:  "host-test-aaa:1",
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
								Node:  "host-test-bbb:1",
								State: pb.PlacementState_PS_READY,
							},
						},
					},
				},
			}, ccc[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPreparing]}", orch.rost.TestString())

	// 2. New range finishes preparing.

	c3PAR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", orch.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc [3:NsPrepared]}", orch.rost.TestString())

	a1PDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsTaking)
	b2PDR := nodes.Get("test-bbb").AddBarrier(t, 2, state.NsTaking)
	tickWait(orch, a1PDR, b2PDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 2}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsReady} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsReady} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaking]} {test-bbb [2:NsTaking]} {test-ccc [3:NsPrepared]}", orch.rost.TestString())

	// 4. Old ranges becomes taken.

	a1PDR.Release()
	b2PDR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsPrepared]}", orch.rost.TestString())

	c3AR := nodes.Get("test-ccc").AddBarrier(t, 3, state.NsReadying)
	tickWait(orch)
	c3AR.Wait()
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; assert.Len(t, ccc, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 3}, ccc[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsPrepared}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReadying]}", orch.rost.TestString())

	// 5. New range becomes ready.

	c3AR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsTaken]} {test-bbb [2:NsTaken]} {test-ccc [3:NsReady]}", orch.rost.TestString())

	a1DR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDropping)
	b2DR := nodes.Get("test-bbb").AddBarrier(t, 2, state.NsDropping)
	tickWait(orch, a1DR, b2DR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 2}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsTaken} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsTaken} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsReady]}", orch.rost.TestString())

	// Drops finish.
	a1DR.Release()
	b2DR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsReady]}", orch.rost.TestString())

	requireStable(t, orch)

	// Assert that the error chan was closed, to indicate op is complete.
	select {
	case err, ok := <-op.Err:
		if ok {
			assert.NoError(t, err)
		}
	default:
		assert.Fail(t, "expected op.Err to be closed")
	}
}

func TestSlowRPC(t *testing.T) {

	// One node, one range, unplaced.

	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()

	nodes.Get("test-aaa").SetGracePeriod(3 * time.Second)

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	// ----

	// No WaitRPCs() this time! But we do stil have to wait until our one
	// expected RPC reaches the barrier (via PrepareAddRange). Otherwise, Tick
	// without WaitRPCs will likely return before the RPC has even started, let
	// alone gotten to the barrier.
	par := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsPreparing)
	orch.Tick()
	par.Wait()

	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			if assert.IsType(t, &pb.GiveRequest{}, aaa[0]) {
				ProtoEqual(t, &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				}, aaa[0].(*pb.GiveRequest).Range)
			}
		}
	}

	// Placement has been moved into PsPending, because we ticked past it...
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	// But the roster hasn't been updated yet, because the RPC hasn't completed.
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// RPC above is still in flight when the next Tick starts!
	// (This is quite unusual for tests.)
	orch.Tick()

	// No redundant RPC this time.
	assert.Empty(t, nodes.RPCs())

	// No state change; nothing happened.
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// Give RPC finally completes! Roster is updated.
	par.Release()
	orch.WaitRPCs()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsPrepared]}", orch.rost.TestString())

	// Subsequent ticks continue the placement as usual. No need to verify the
	// details in this test.
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsReady]}", orch.rost.TestString())
}

// ----------------------------------------------------------- fixture templates

func initTestPlacement(t *testing.T, strict bool) (*Orchestrator, *fake_nodes.TestNodes) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strict)
	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())
	return orch, nodes
}

// initTestMove spawns two hosts (aaa, bbb), one range (1), and one placement
// (range 1 is on aaa in PsReady), and returns the range.
func initTestMove(t *testing.T, strict bool) (*Orchestrator, *fake_nodes.TestNodes, *ranje.Range) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}"
	rosStr := "{test-aaa [1:NsReady]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strict)

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	r1 := mustGetRange(t, orch.ks, 1)
	return orch, nodes, r1
}

func initTestSplit(t *testing.T, strict bool) (*Orchestrator, *fake_nodes.TestNodes, *ranje.Range) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsReady}"
	rosStr := "{test-aaa [1:NsReady]}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strict)

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	r1 := mustGetRange(t, orch.ks, 1)
	return orch, nodes, r1
}

// initTestJoin sets up three hosts (aaa, bbb, ccc), two ranges (1, 2) split at
// ggg, and two placements (r1 on aaa, r2 on bbb; both in PsReady), and returns
// the two ranges.
func initTestJoin(t *testing.T, strict bool) (*Orchestrator, *fake_nodes.TestNodes, *ranje.Range, *ranje.Range) {

	// Start with two ranges (which together cover the whole keyspace) assigned
	// to two of three nodes. The ranges will be joined onto the third node.

	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}"
	rosStr := "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strict)

	// Probe the fake nodes to verify the setup.

	orch.rost.Tick()
	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())

	r1 := mustGetRange(t, orch.ks, 1)
	r2 := mustGetRange(t, orch.ks, 2)
	return orch, nodes, r1, r2
}

// ----------------------------------------------------------- fixture factories

func testConfig() config.Config {
	return config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // ~never
		Replication:              1,
	}
}

type rangeStub struct {
	rID        string
	startKey   string
	endKey     string
	rState     string
	placements []placementStub
}

type placementStub struct {
	nodeID string
	pState string
}

type nodePlacementStub struct {
	rID    int
	nState string
}

type nodeStub struct {
	nodeID     string
	placements []nodePlacementStub
}

func parseKeyspace(t *testing.T, keyspace string) []rangeStub {

	// keyspace = "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady} {2 (ggg, +inf] RsActive p0=test-bbb:PsReady}"
	// roster = "{test-aaa [1:NsReady]} {test-bbb [2:NsReady]} {test-ccc []}"

	r := regexp.MustCompile(`{[^{}]*}`)
	x := r.FindAllString(keyspace, -1)

	sr := make([]rangeStub, len(x))
	for i := range x {
		fmt.Printf("x[%d]: %s\n", i, x[i])

		// {1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}
		//                       {    1                [          -inf            ,      ggg             ]                RsActive    (placements)   }
		r = regexp.MustCompile(`^{` + `(\d+)` + ` ` + `[\[\(]` + `([\+\-\w]+)` + `, ` + `([\+\-\w]+)` + `[\]\)]` + ` ` + `(Rs\w+)` + `(?: (.+))?` + `}$`)
		y := r.FindStringSubmatch(x[i])
		if y == nil {
			t.Fatalf("invalid range string: %v", x[i])
		}

		sr[i] = rangeStub{
			rID:      y[1],
			startKey: y[2],
			endKey:   y[3],
			rState:   y[4],
		}

		placements := y[5]
		if placements != "" {
			pl := strings.Split(placements, ` `)
			sp := make([]placementStub, len(pl))
			for ii := range pl {
				// p0=test-aaa:PsReady
				//                            p0         =     test-aaa :     PsReady
				r = regexp.MustCompile(`^` + `p(\d+)` + `=` + `(.+)` + `:` + `(Ps\w+)` + `$`)
				z := r.FindStringSubmatch(pl[ii])
				if z == nil {
					t.Fatalf("invalid placement string: %v", pl[ii])
				}

				// TODO: Check that indices are contiguous?

				sp[ii] = placementStub{nodeID: z[2], pState: z[3]}
			}

			sr[i].placements = sp
		}
	}

	return sr
}

func TestParseKeyspace(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	ks := keyspaceFactory(t, config.Config{}, parseKeyspace(t, ksStr))
	assert.Equal(t, ksStr, ks.LogString())
}

func parseRoster(t *testing.T, s string) []nodeStub {

	// {aa} {bb}
	r1 := regexp.MustCompile(`{[^{}]*}`)

	// {test-aaa [1:NsReady 2:NsReady]}
	//                         {     test-aaa            [1:NsReady 2:NsReady]}
	r2 := regexp.MustCompile(`^{` + `([\w\-]+)` + ` ` + `\[(.*)\]}$`)

	// 1:NsReady
	r3 := regexp.MustCompile(`^` + `(\d+)` + `:` + `(Ns\w+)` + `$`)

	x := r1.FindAllString(s, -1)

	ns := make([]nodeStub, len(x))
	for i := range x {
		fmt.Printf("x[%d]: %s\n", i, x[i])

		y := r2.FindStringSubmatch(x[i])
		if y == nil {
			t.Fatalf("invalid node string: %v", x[i])
		}

		ns[i] = nodeStub{
			nodeID:     y[1],
			placements: []nodePlacementStub{},
		}

		placements := y[2]
		if placements != "" {
			pl := strings.Split(placements, ` `)
			nps := make([]nodePlacementStub, len(pl))
			for ii := range pl {
				z := r3.FindStringSubmatch(pl[ii])
				if z == nil {
					t.Fatalf("invalid placement string: %v", pl[ii])
				}
				rID, err := strconv.Atoi(z[1])
				if err != nil {
					t.Fatalf("invalid parsing range ID: %v", err)
				}
				nps[ii] = nodePlacementStub{
					rID:    rID,
					nState: z[2],
				}
			}
			ns[i].placements = nps
		}
	}

	return ns
}

func nodesFactory(t *testing.T, cfg config.Config, ctx context.Context, ks *keyspace.Keyspace, stubs []nodeStub) (*fake_nodes.TestNodes, *roster.Roster) {
	nodes := fake_nodes.NewTestNodes()
	rost := roster.New(cfg, nodes.Discovery(), nil, nil, nil)

	for i := range stubs {

		rem := discovery.Remote{
			Ident: stubs[i].nodeID,
			Host:  fmt.Sprintf("host-%s", stubs[i].nodeID),
			Port:  1,
		}

		ri := map[ranje.Ident]*info.RangeInfo{}
		for _, pStub := range stubs[i].placements {

			rID := ranje.Ident(pStub.rID)
			r, err := ks.Get(rID)
			if err != nil {
				t.Fatalf("invalid node placement stub: %v", err)
			}

			ri[rID] = &info.RangeInfo{
				Meta:  r.Meta,
				State: RemoteStateFromString(t, pStub.nState),
			}
		}

		nodes.Add(ctx, rem, ri)
	}

	rost.NodeConnFactory = nodes.NodeConnFactory
	return nodes, rost
}

func orchFactory(t *testing.T, sKS, sRos string, cfg config.Config, strict bool) (*Orchestrator, *fake_nodes.TestNodes) {
	ks := keyspaceFactory(t, cfg, parseKeyspace(t, sKS))
	nodes, ros := nodesFactory(t, cfg, context.TODO(), ks, parseRoster(t, sRos))
	nodes.SetStrictTransitions(strict)
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	orch := New(cfg, ks, ros, srv)
	return orch, nodes
}

func TestParseRoster(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	ks := keyspaceFactory(t, config.Config{}, parseKeyspace(t, ksStr))
	assert.Equal(t, ksStr, ks.LogString())

	rosStr := "{test-aaa [1:NsReady 2:NsReady]} {test-bbb []} {test-ccc []}"
	fmt.Printf("%v\n", parseRoster(t, rosStr))
	nodes, ros := nodesFactory(t, testConfig(), context.TODO(), ks, parseRoster(t, rosStr))
	defer nodes.Close()

	ros.Tick()
	assert.Equal(t, rosStr, ros.TestString())
}

func TestOrchFactory(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsReady p1=test-bbb:PsReady} {2 (ggg, +inf] RsActive}"
	rosStr := "{test-aaa [1:NsReady 2:NsReady]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), false)
	defer nodes.Close()
	orch.rost.Tick()

	assert.Equal(t, ksStr, orch.ks.LogString())
	assert.Equal(t, rosStr, orch.rost.TestString())
}

func PlacementStateFromString(t *testing.T, s string) ranje.PlacementState {
	switch s {
	case ranje.PsUnknown.String():
		return ranje.PsUnknown

	case ranje.PsPending.String():
		return ranje.PsPending

	case ranje.PsPrepared.String():
		return ranje.PsPrepared

	case ranje.PsReady.String():
		return ranje.PsReady

	case ranje.PsTaken.String():
		return ranje.PsTaken

	case ranje.PsGiveUp.String():
		return ranje.PsGiveUp

	case ranje.PsDropped.String():
		return ranje.PsDropped
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return ranje.PsUnknown // unreachable
}

func RemoteStateFromString(t *testing.T, s string) state.RemoteState {
	switch s {
	case "NsUnknown":
		return state.NsUnknown
	case "NsPreparing":
		return state.NsPreparing
	case "NsPreparingError":
		return state.NsPreparingError
	case "NsPrepared":
		return state.NsPrepared
	case "NsReadying":
		return state.NsReadying
	case "NsReadyingError":
		return state.NsReadyingError
	case "NsReady":
		return state.NsReady
	case "NsTaking":
		return state.NsTaking
	case "NsTakingError":
		return state.NsTakingError
	case "NsTaken":
		return state.NsTaken
	case "NsDropping":
		return state.NsDropping
	case "NsDroppingError":
		return state.NsDroppingError
	case "NsNotFound":
		return state.NsNotFound
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return state.NsUnknown // unreachable
}

// TODO: Remove config param. Config was a mistake.
func keyspaceFactory(t *testing.T, cfg config.Config, stubs []rangeStub) *keyspace.Keyspace {
	ranges := make([]*ranje.Range, len(stubs))
	for i := range stubs {
		r := ranje.NewRange(ranje.Ident(i + 1))
		r.State = ranje.RsActive

		if i > 0 {
			r.Meta.Start = ranje.Key(stubs[i].startKey)
			ranges[i-1].Meta.End = ranje.Key(stubs[i].startKey)
		}

		r.Placements = make([]*ranje.Placement, len(stubs[i].placements))

		for ii := range stubs[i].placements {
			pstub := stubs[i].placements[ii]
			r.Placements[ii] = &ranje.Placement{
				NodeID: pstub.nodeID,
				State:  PlacementStateFromString(t, pstub.pState),
			}
		}

		ranges[i] = r
	}

	pers := &FakePersister{ranges: ranges}

	var err error
	ks, err := keyspace.New(cfg, pers)
	if err != nil {
		t.Fatalf("keyspace.New: %s", err)
	}

	return ks
}

// --------------------------------------------------------------------- helpers

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

func tickUntilStable(t *testing.T, orch *Orchestrator, nodes *fake_nodes.TestNodes) {
	var ksPrev string // previous value of ks.LogString
	var stable int    // ticks since keyspace changed or rpc sent
	var ticks int     // total ticks waited

	for {
		tickWait(orch)
		rpcs := len(nodes.RPCs())

		// Keyspace changed since last tick, or RPCs sent? Keep ticking.
		// TODO: Can we check whether RPCs were sent without having a the
		//       TestNodes? Would be simpler to do this with just the Orch.
		ksNow := orch.ks.LogString()
		if ksPrev != ksNow || rpcs > 0 {
			ksPrev = ksNow
			stable = 0
		} else {
			stable += 1
		}

		// Stable for a few ticks? We're done.
		if stable >= 2 {
			break
		}

		ticks += 1
		if ticks > 50 {
			t.Fatal("didn't stablize after 50 ticks")
			return
		}
	}

	// Perform a single probe cycle before returning, to update the remote
	// state of all nodes. (Otherwise we're only observing the remote state as
	// returned from the RPCs. Any state changes which happened outside of those
	// RPCs will be missed.)
	orch.rost.Tick()
}

func requireStable(t *testing.T, orch *Orchestrator) {
	ksLog := orch.ks.LogString()
	rostLog := orch.rost.TestString()
	for i := 0; i < 2; i++ {
		tickWait(orch)
		orch.rost.Tick()
		// Use require (vs assert) since spamming the same error doesn't help.
		require.Equal(t, ksLog, orch.ks.LogString())
		require.Equal(t, rostLog, orch.rost.TestString())
	}
}

// ProtoEqual is a helper to compare two slices of protobufs. It's not great.
func ProtoEqual(t *testing.T, expected, actual interface{}) {
	if diff := cmp.Diff(expected, actual, protocmp.Transform()); diff != "" {
		t.Errorf(fmt.Sprintf("Not equal (-want +got):\n%s\n", diff))
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

// mustGetRange returns a range from the given keyspace or fails the test.
func mustGetRange(t *testing.T, ks *keyspace.Keyspace, rID int) *ranje.Range {
	r, err := ks.Get(ranje.Ident(rID))
	if err != nil {
		t.Fatalf("ks.Get(%d): %v", rID, err)
	}
	return r
}

// ------------------------------------------------------------------- persister

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
