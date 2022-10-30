package orchestrator

import (
	"fmt"
	"log"
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
	"github.com/adammck/ranger/pkg/test/fake_node"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
)

const strictTransactions = true
const noStrictTransactions = false

// Note: These tests are very verbose, but the orchestrator is the most critical
// part of Ranger. When things go wrong here (e.g. storage nodes getting into a
// weird combination of state and not being able to recover), we're immediately
// in sev-1 territory. So let's be sure that it works.
//
// Each of the four main operations are tested:
// - Place
// - Move
// - Split
// - Join
//
// For each of those, there are three variants of the happy path:
//
// - Short: Just set things up, tickUntilStable, and assert that things the
//          keyspace and roster look as expected. This is just a sanity check.
//
// - Normal: Set things up like Short, but explicity perform each Tick, and
//           assert that the expected RPCs are sent, and that the keyspace and
//           roster advance in the expected manner. This is quite brittle to
//           code changes compared to Short, but we want to be sure that there
//           aren't a bunch of unexpected intermediate states along the way.
//
// - Slow: Same as Normal, except that command RPCs (PrepareAddRange, etc) all
//         take longer than the grace period, and so we see the intermediate
//         remote states (NsLoading, etc).
//
// In addition to the happy path, we want to test what happens when each of the
// commands (as detailed in the Normal variant, above) fails. Take a look in the
// docs directory for more.

// TODO: Move to keyspace tests.
func TestJunk(t *testing.T) {
	ksStr := "" // No ranges at all!
	orch, nodes := orchFactoryNoCheck(t, ksStr, "", testConfig(), noStrictTransactions)
	defer nodes.Close()

	// Genesis range was created.
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())

	// This really just restates the above.
	r := mustGetRange(t, orch.ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	assert.Equal(t, ranje.RsActive, r.State, "range should be born active")
	assert.Equal(t, 0, len(r.Placements), "range should be born with no placements")
}

func TestPlace(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
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

	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	// Second tick: Keyspace is updated with state from roster. No RPCs sent.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	// Third: Serve RPC is sent, to advance to ready. Returns success, and
	// roster is updated. Keyspace is not.

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	// Forth: Keyspace is updated with ready state from roster. No RPCs sent.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	// No more changes. This is steady state.

	requireStable(t, orch)
}

func TestPlace_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
}

func TestPlace_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	defer nodes.Close()

	par := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsLoading)
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
	assert.Equal(t, "{test-aaa [1:NsLoading]}", orch.rost.TestString())
	// TODO: Assert that new placement was persisted

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsLoading]}", orch.rost.TestString())

	// The node finished preparing, but we don't know about it.
	par.Release()
	assert.Equal(t, "{test-aaa [1:NsLoading]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	//
	// TODO: Maybe the state update should immediately trigger another tick just
	//       for that placement? Would save a tick, but risks infinite loops.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())

	ar := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsActivating)
	tickWait(orch, ar)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActivating]}", orch.rost.TestString())

	// The node became ready, but as above, we don't know about it.
	ar.Release()
	assert.Equal(t, "{test-aaa [1:NsActivating]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestPlaceFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// Node aaa will always fail PrepareAddRange.
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.PrepareAddRange, fmt.Errorf("something went wrong"))

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

// TODO: Maybe remove this test since we have the long version below?
func TestPlaceFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// AddRange will always fail on node A.
	// (But PrepareAddRange will succeed, as is the default)
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.AddRange, fmt.Errorf("can't get ready!"))

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestPlaceFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// AddRange will always fail on node A.
	// (But PrepareAddRange will succeed, as is the default)
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.AddRange, fmt.Errorf("can't get ready!"))

	// 1. PrepareAddRange(1, aaa)

	tickWait(orch)
	assert.Len(t, nodes.RPCs(), 1) // no need to check RPC contents again.
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())

	// 2. AddRange(1, aaa)
	//    Makes three attempts.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(orch)
		assert.Len(t, nodes.RPCs(), 1) // no need to check RPC contents again.
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())

		p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
		assert.Equal(t, attempt, p.Attempts)
		assert.False(t, p.GivenUpOnActivate)
	}

	tickWait(orch)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())
	assert.True(t, mustGetPlacement(t, orch.ks, 1, "test-aaa").GivenUpOnActivate)

	// 3. DropRange(1, aaa)

	tickWait(orch)
	require.Equal(t, "Drop(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []}", orch.rost.TestString())

	// 4. PrepareAddRange(1, bbb)
	// 5. AddRange(1, bbb)

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestMove(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	func() {
		orch.opMovesMu.Lock()
		defer orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		orch.opMoves = append(orch.opMoves, OpMove{
			Range: 1,
			Dest:  "test-bbb",
		})
	}()

	tickWait(orch)
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
								State: pb.PlacementState_PS_ACTIVE,
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

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Take(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Serve(R1, test-bbb)", rpcsToString(nodes.RPCs()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Drop(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// p0 is gone!
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestMove_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	op := OpMove{
		Range: ranje.Ident(1),
		Dest:  "test-bbb",
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	tickUntilStable(t, orch, nodes)

	// Range moved from aaa to bbb.
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
}

func TestMove_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	func() {
		orch.opMovesMu.Lock()
		defer orch.opMovesMu.Unlock()
		// TODO: Probably add a method to do this.
		orch.opMoves = append(orch.opMoves, OpMove{
			Range: 1,
			Dest:  "test-bbb",
		})
	}()

	bPAR := nodes.Get("test-bbb").AddBarrier(t, 1, state.NsLoading)
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
								State: pb.PlacementState_PS_ACTIVE,
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

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsLoading]}", orch.rost.TestString())

	// Node B finished preparing.
	bPAR.Release()
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsLoading]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	aPDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDeactivating)
	tickWait(orch, aPDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Take
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	aPDR.Release() // Node A finished taking.
	assert.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	bAR := nodes.Get("test-bbb").AddBarrier(t, 1, state.NsActivating)
	tickWait(orch, bAR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 1}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActivating]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActivating]}", orch.rost.TestString())

	bAR.Release() // Node B finished becoming ready.
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActivating]}", orch.rost.TestString())

	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	aDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDropping)
	tickWait(orch, aDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	aDR.Release() // Node A finished dropping.
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// test-aaa is gone!
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestMoveFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// PrepareAddRange will always fail on test-bbb.
	nodes.Get("test-bbb").SetReturnValue(t, 1, fake_node.PrepareAddRange, fmt.Errorf("failure injected by TestMoveFailure_PrepareAddRange"))

	// ----

	op := OpMove{
		Range: ranje.Ident(1),
		Dest:  "test-bbb",
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	// ----

	// 1. PrepareAddRange(1, bbb)
	//    Makes three attempts, which will all fail because we stubbed them to
	//    to do so, above.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(orch)
		if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
			if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
				if assert.IsType(t, &pb.GiveRequest{}, bbb[0]) {
					ProtoEqual(t, &pb.RangeMeta{
						Ident: 1,
						Start: []byte(ranje.ZeroKey),
						End:   []byte(ranje.ZeroKey),
					}, bbb[0].(*pb.GiveRequest).Range)
				}
			}
		}
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsNotFound]}", orch.rost.TestString())
		assert.Equal(t, attempt, mustGetPlacement(t, orch.ks, 1, "test-bbb").Attempts)
	}

	// Failed placement is destroyed.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsNotFound]}", orch.rost.TestString())

	// Placement is cleaned up after next probe cycle.

	orch.rost.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Done.

	requireStable(t, orch)
}

func TestMoveFailure_PrepareDropRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// PrepareDropRange will always fail on node A.
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.PrepareDropRange, fmt.Errorf("can't take! nobody knows why!"))

	// ----

	op := OpMove{
		Range: ranje.Ident(1),
		Dest:  "test-bbb",
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	// ----

	// 1. Node B gets PrepareAddRange to verify that it can take the shard. This
	//    succeeds (because nothing has failed yet).
	log.Print("1")

	tickWait(orch)
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
								State: pb.PlacementState_PS_ACTIVE,
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

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Keyspace updates from roster.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 2. Node A gets PrepareDropRange, which fails because we injected an error
	//    above. This repeats three times before we give up and accept that the
	//    node will not relinquish the range.
	log.Print("2")

	for i := 1; i < 4; i++ {
		log.Printf("2.%d", i)
		tickWait(orch)
		if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
			if a := rpcs["test-aaa"]; assert.Len(t, a, 1) {
				ProtoEqual(t, &pb.TakeRequest{
					Range: 1,
				}, a[0])
			}
		}

		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())
	}

	// 3. Node B gets DropRange, to abandon the placement it prepared. It will
	//    never become ready.
	log.Print("3")

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if b := rpcs["test-bbb"]; assert.Len(t, b, 1) {
			ProtoEqual(t, &pb.DropRequest{
				Range: 1,
			}, b[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Destroy the abandonned placement.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsDropped:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Destroy the abandonned placement.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Stable now, because the move was a one-off. (The balancer might try the
	// same thing again, but that's a separate test.)
	requireStable(t, orch)
}

func TestMoveFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

	// AddRange will always fail on test-bbb.
	nodes.Get("test-bbb").SetReturnValue(t, 1, fake_node.AddRange, fmt.Errorf("failure injected by TestMoveFailure_AddRange"))

	// ----

	op := OpMove{
		Range: ranje.Ident(1),
		Dest:  "test-bbb",
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	// ----

	// 1. PrepareAddRange(1, bbb)
	log.Print("1")

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			if assert.IsType(t, &pb.GiveRequest{}, bbb[0]) {
				ProtoEqual(t, &pb.RangeMeta{
					Ident: 1,
					Start: []byte(ranje.ZeroKey),
					End:   []byte(ranje.ZeroKey),
				}, bbb[0].(*pb.GiveRequest).Range)
			}
		}
	}
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 2. PrepareDropRange(1, aaa)
	log.Print("2")

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			if assert.IsType(t, &pb.TakeRequest{}, aaa[0]) {
				assert.Equal(t, uint64(1), aaa[0].(*pb.TakeRequest).Range)
			}
		}
	}
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// TODO: Why doesn't this step happen? Think it's the placement-ordering
	//       thing where some stuff happens in one step instead of two if
	//       dependent placements are ordered a certain way. Need to perform two
	//       separate steps: update keyspace, then send RPCs.

	// tickWait(orch)
	// assert.Empty(t, nodes.RPCs())
	// assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	// assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 3. AddRange(1, bbb)
	//    Makes three attempts, which will all fail because we stubbed them to
	//    to do so, above.
	log.Print("3")

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(orch)
		if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
			if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
				if assert.IsType(t, &pb.ServeRequest{}, bbb[0]) {
					assert.Equal(t, uint64(1), bbb[0].(*pb.ServeRequest).Range)
				}
			}
		}
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

		p := mustGetPlacement(t, orch.ks, 1, "test-bbb")
		assert.Equal(t, attempt, p.Attempts)
		assert.False(t, p.GivenUpOnActivate)
	}

	// Replacement (on bbb) is marked as GivenUp.
	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())
	assert.True(t, mustGetPlacement(t, orch.ks, 1, "test-bbb").GivenUpOnActivate)

	// 4. AddRange(1, aaa)
	log.Print("4")

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			if assert.IsType(t, &pb.ServeRequest{}, aaa[0]) {
				assert.Equal(t, uint64(1), aaa[0].(*pb.ServeRequest).Range)
			}
		}
	}
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// TODO: This step is also merged with the next :|
	// tickWait(orch)
	// assert.Empty(t, nodes.RPCs())
	// assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	// assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 5. DropRange(1, bbb)
	log.Print("5")

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			if assert.IsType(t, &pb.DropRequest{}, bbb[0]) {
				assert.Equal(t, uint64(1), bbb[0].(*pb.DropRequest).Range)
			}
		}
	}
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	requireStable(t, orch)
}

func TestMoveFailure_DropRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// DropRange will always fail on test-aaa (src).
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.DropRange, fmt.Errorf("failure injected by TestMoveFailure_DropRange"))

	// ----

	op := OpMove{
		Range: ranje.Ident(1),
		Dest:  "test-bbb",
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	// Fast-forward to the part where we send DropRange to aaa.
	tickUntil(t, orch, nodes, func(ks, ro string) bool {
		return (true &&
			ks == "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}" &&
			ro == "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}")
	})

	// ----

	for attempt := 1; attempt <= 5; attempt++ {
		tickWait(orch)

		// The RPC was sent.
		if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
			if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
				if assert.IsType(t, &pb.DropRequest{}, aaa[0]) {
					assert.Equal(t, uint64(1), aaa[0].(*pb.DropRequest).Range)
				}
			}
		}

		// No state changed.
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

		// (Except for this counter.)
		assert.Equal(t, attempt, mustGetPlacement(t, orch.ks, 1, "test-aaa").DropAttempts)
	}

	// Not checking stability here. Failing to drop will retry forever until an
	// operator intervenes to force the node to drop the placement. This hack
	// pretends that that happened, so we can observe the workflow unblocking.
	nodes.Get("test-aaa").ForceDrop(1)

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestSplit(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// 0. Initiate

	opErr := splitOp(orch, 1)

	// 1. PrepareAddRange

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	tickWait(orch)
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
								State: pb.PlacementState_PS_ACTIVE,
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
								State: pb.PlacementState_PS_ACTIVE,
							},
						},
					},
				},
			}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// 2. PrepareDropRange

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// 3. AddRange

	tickWait(orch)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 2) {
			ProtoEqual(t, &pb.ServeRequest{Range: 2}, aaa[0])
			ProtoEqual(t, &pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())

	// 4. DropRange

	tickWait(orch)
	require.Equal(t, "Drop(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	// 5. Cleanup

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
	assertClosed(t, opErr)
}

func TestSplit_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	splitOp(orch, 1)
	tickUntilStable(t, orch, nodes)

	// Range 1 was split into ranges 2 and 3 at ccc.
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
}

func TestSplit_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	defer nodes.Close()
	requireStable(t, orch)
	opErr := splitOp(orch, 1)

	// 1. Split initiated by controller. Node hasn't heard about it yet.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	// 2. Controller places new ranges on nodes.

	a2PAR := nodes.Get("test-aaa").AddBarrier(t, 2, state.NsLoading)
	a3PAR := nodes.Get("test-aaa").AddBarrier(t, 3, state.NsLoading)
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
								State: pb.PlacementState_PS_ACTIVE,
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
								State: pb.PlacementState_PS_ACTIVE,
							},
						},
					},
				},
			}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsLoading 3:NsLoading]}", orch.rost.TestString())

	// 3. Wait for placements to become Prepared.

	a2PAR.Release() // R2 finished preparing, but R3 has not yet.
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsLoading 3:NsLoading]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 2) // redundant Gives
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	a3PAR.Release() // R3 becomes Prepared, too.
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsLoading]}", orch.rost.TestString())

	tickWait(orch)
	// Note that we're not sending (redundant) Give RPCs to R2 any more.
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Give
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	// 4. Controller takes placements in parent range.

	a1PDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDeactivating)
	tickWait(orch, a1PDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDeactivating 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	a1PDR.Release() // r1p0 finishes taking.
	assert.Equal(t, "{test-aaa [1:NsDeactivating 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Take
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// 5. Controller instructs both child ranges to become Ready.
	a2AR := nodes.Get("test-aaa").AddBarrier(t, 2, state.NsActivating)
	a3AR := nodes.Get("test-aaa").AddBarrier(t, 3, state.NsActivating)
	tickWait(orch, a2AR, a3AR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 2) {
			ProtoEqual(t, &pb.ServeRequest{Range: 2}, aaa[0])
			ProtoEqual(t, &pb.ServeRequest{Range: 3}, aaa[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActivating]}", orch.rost.TestString())

	a3AR.Release() // r3p0 becomes ready.
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActivating]}", orch.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 2) // redundant Serves
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	a2AR.Release() // r2p0 becomes ready.
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActive]}", orch.rost.TestString())

	// Orchestrator notices on next tick.
	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Serve
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	// 6. Orchestrator instructs parent range to drop placements.

	a1DR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDropping)
	tickWait(orch, a1DR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.DropRequest{Range: 1}, aaa[0])
		}
	}

	assert.Equal(t, "{test-aaa [1:NsDropping 2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{test-aaa [1:NsDropping 2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	a1DR.Release() // r1p0 finishes dropping.

	tickWait(orch)
	assert.Len(t, RPCs(nodes.RPCs()), 1) // redundant Drop
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	// Whenever the next probe cycle happens, we notice that the range is gone
	// from the node, because it was dropped. Orchestrator doesn't notice this,
	// but maybe should, after sending the redundant Drop RPC?
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
	assertClosed(t, opErr)
}

func TestSplitFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// Left side of the split range will not be accepted by bbb. It will be retried on ccc.
	nodes.Get("test-bbb").SetReturnValue(t, 2, fake_node.PrepareAddRange, fmt.Errorf("something went wrong"))

	// 0. Initiate

	splitOp(orch, 1)

	// 1. PrepareAddRange

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

	tickWait(orch)
	// First tick attempts to give both placements.
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 2) {
			assert.IsType(t, &pb.GiveRequest{}, bbb[0])
			assert.IsType(t, &pb.GiveRequest{}, bbb[1])
		}
	}

	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsNotFound 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	assert.Equal(t, 1, mustGetPlacement(t, orch.ks, 2, "test-bbb").Attempts)
	assert.Equal(t, 1, mustGetPlacement(t, orch.ks, 3, "test-bbb").Attempts)

	for attempt := 2; attempt <= 3; attempt++ {
		tickWait(orch)
		// Only the failing placement (rID=2) will be retried.
		if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-bbb"}, nIDs(rpcs)) {
			if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
				if assert.IsType(t, &pb.GiveRequest{}, bbb[0]) {
					assert.Equal(t, uint64(2), bbb[0].(*pb.GiveRequest).Range.Ident)
				}
			}
		}

		assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsNotFound 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
		assert.Equal(t, attempt, mustGetPlacement(t, orch.ks, 2, "test-bbb").Attempts)
	}

	tickWait(orch)
	// Failed placement is destroyed.
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsNotFound 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 2. PrepareAddRange (retry on ccc)

	tickWait(orch)
	// Only the failed placement (rID=2) will be retried.
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; assert.Len(t, ccc, 1) {
			if assert.IsType(t, &pb.GiveRequest{}, ccc[0]) {
				assert.Equal(t, uint64(2), ccc[0].(*pb.GiveRequest).Range.Ident)
			}
		}
	}

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-ccc:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsNotFound 3:NsInactive]} {test-ccc [2:NsInactive]}", orch.rost.TestString())

	// Recovered! Finish the split.

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_PrepareDropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The node where the range lives will not relinquish it!
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.PrepareDropRange, fmt.Errorf("failed PrepareDropRange for test"))

	splitOp(orch, 1)

	// End up in a bad but stable situation where the original range never
	// relinquish (that's the point), but that the successors don't activate.
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// R1 is stuck until some operator comes and unsticks it.
	// TODO: Make it possible (configurable) to automatically force drop it.
	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.GiveUpOnDeactivate)
}

func TestSplitFailure_PrepareDropRange(t *testing.T) {
	t.Skip("not implemented")
}

func TestSplitFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// AddRange will always fail on node A.
	// (But PrepareAddRange will succeed, as is the default)
	nodes.Get("test-bbb").SetReturnValue(t, 2, fake_node.AddRange, fmt.Errorf("failed AddRange for test"))

	splitOp(orch, 1)

	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)
	nodes.Get("test-bbb").SetReturnValue(t, 2, fake_node.AddRange, fmt.Errorf("failed AddRange for test"))
	splitOp(orch, 1)

	tickWait(orch)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

	// 1. PrepareAddRange

	tickWait(orch)
	require.Equal(t, "Give(R2, test-bbb), Give(R3, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 2. PrepareDropRange

	tickWait(orch)
	require.Equal(t, "Take(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 3. AddRange

	tickWait(orch)
	require.Equal(t, "Serve(R2, test-bbb), Serve(R3, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsActive]} {test-ccc []}", orch.rost.TestString())

	// TODO: Rename to e.g. ServeRequests to differentiate.
	require.Equal(t, 1, mustGetPlacement(t, orch.ks, 2, "test-bbb").Attempts)
	require.Equal(t, 1, mustGetPlacement(t, orch.ks, 3, "test-bbb").Attempts)

	// Two more attempts to serve R2.
	for attempt := 2; attempt <= 3; attempt++ {
		tickWait(orch)
		require.Equal(t, "Serve(R2, test-bbb)", rpcsToString(nodes.RPCs()))
		require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsActive]} {test-ccc []}", orch.rost.TestString())

		// TODO: Rename Attemps to e.g. AddRangeAttempts.
		require.Equal(t, attempt, mustGetPlacement(t, orch.ks, 2, "test-bbb").Attempts)
	}

	// 4. PrepareDropRange
	// Undo the Serve that succeeded, so we can reactivate the predecessor while
	// a new placement is found for the Serve that failed. Give can be slow, but
	// Take and Serve should be fast.

	tickWait(orch)
	require.Equal(t, "Take(R3, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.True(t, mustGetPlacement(t, orch.ks, 2, "test-bbb").GivenUpOnActivate)

	tickWait(orch)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 5. AddRange

	tickWait(orch)
	require.Equal(t, "Serve(R1, test-aaa)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 6. DropRange

	tickWait(orch)
	require.Equal(t, "Drop(R2, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 7. PrepareAddRange (retry)

	tickWait(orch)
	require.Equal(t, "Give(R2, test-ccc)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-ccc:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc [2:NsInactive]}", orch.rost.TestString())

	// Recovered! Let the re-placement of R3 on Nccc finish.

	tickUntilStable(t, orch, nodes)
	require.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_DropRange(t *testing.T) {
	t.Skip("not implemented")
}

func TestSplitFailure_DropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The source node will not drop the range.
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.DropRange, fmt.Errorf("failed DropRange for test"))

	splitOp(orch, 1)

	// End up in a bad but stable situation where the original range never
	// relinquish (that's the point), but that the successors don't activate.
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsSplitting p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsActive 3:NsActive]} {test-ccc []}", orch.rost.TestString())

	// R1 is stuck until some operator comes and unsticks it.
	// TODO: Make it possible (configurable) to automatically force drop it.
	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.DropFailed)
}

func TestJoin(t *testing.T) {
	t.Skip("not implemented")
}

func TestJoin_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	op := OpJoin{
		Left:  1,
		Right: 2,
		Dest:  "test-ccc",
		Err:   make(chan error),
	}

	orch.opJoinsMu.Lock()
	orch.opJoins = append(orch.opJoins, op)
	orch.opJoinsMu.Unlock()

	tickUntilStable(t, orch, nodes)

	// Ranges 1 and 2 were joined into range 3, which holds the entire keyspace.
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())
}

func TestJoin_Slow(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	opErr := joinOp(orch, 1, 2, "test-ccc")

	// 1. Controller initiates join.

	c3PAR := nodes.Get("test-ccc").AddBarrier(t, 3, state.NsLoading)
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
								State: pb.PlacementState_PS_ACTIVE,
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
								State: pb.PlacementState_PS_ACTIVE,
							},
						},
					},
				},
			}, ccc[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsLoading]}", orch.rost.TestString())

	// 2. New range finishes preparing.

	c3PAR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// 3. Controller takes the ranges from the source nodes.

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	a1PDR := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsDeactivating)
	b2PDR := nodes.Get("test-bbb").AddBarrier(t, 2, state.NsDeactivating)
	tickWait(orch, a1PDR, b2PDR)
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-aaa", "test-bbb"}, nIDs(rpcs)) {
		if aaa := rpcs["test-aaa"]; assert.Len(t, aaa, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 1}, aaa[0])
		}
		if bbb := rpcs["test-bbb"]; assert.Len(t, bbb, 1) {
			ProtoEqual(t, &pb.TakeRequest{Range: 2}, bbb[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [2:NsDeactivating]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// 4. Old ranges becomes taken.

	a1PDR.Release()
	b2PDR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	c3AR := nodes.Get("test-ccc").AddBarrier(t, 3, state.NsActivating)
	tickWait(orch)
	c3AR.Wait()
	if rpcs := nodes.RPCs(); assert.Equal(t, []string{"test-ccc"}, nIDs(rpcs)) {
		if ccc := rpcs["test-ccc"]; assert.Len(t, ccc, 1) {
			ProtoEqual(t, &pb.ServeRequest{Range: 3}, ccc[0])
		}
	}

	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsInactive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActivating]}", orch.rost.TestString())

	// 5. New range becomes ready.

	c3AR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsInactive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]}", orch.rost.TestString())

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

	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsInactive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsActive]}", orch.rost.TestString())

	// Drops finish.
	a1DR.Release()
	b2DR.Release()
	orch.rost.Tick()
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsDropped} {2 (ggg, +inf] RsJoining p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(orch)
	assert.Empty(t, nodes.RPCs())
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	requireStable(t, orch)
	assertClosed(t, opErr)
}

func TestJoinFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []} {test-ddd []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The new range will not be accepted by ccc.
	nodes.Get("test-ccc").SetReturnValue(t, 3, fake_node.PrepareAddRange, fmt.Errorf("something went wrong"))

	// 0. Initiate

	_ = joinOp(orch, 1, 2, "test-ccc")

	// 1. PrepareAddRange
	// Makes three attempts.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(orch)
		require.Len(t, nodes.RPCs(), 1) // no need to check RPC contents again. It's a Give to test-ccc.
		require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsNotFound]} {test-ddd []}", orch.rost.TestString())

		p := mustGetPlacement(t, orch.ks, 3, "test-ccc")
		require.Equal(t, attempt, p.Attempts)
		require.False(t, p.GivenUpOnActivate)
	}

	// Gave up on test-ccc...

	tickWait(orch)
	//assertClosed(t, opErr) // <- TODO(adammck): Fix this.
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsNotFound]} {test-ddd []}", orch.rost.TestString())

	// But for better or worse, R3 now exists, and the orchestrator will try to
	// place it rather than giving up altogether and abandonning the range. I'm
	// not sure if that's right -- maybe it'd be better to just give up and set
	// the predecessors back to RsActive? -- but this way Just Works.

	tickUntilStable(t, orch, nodes)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ddd:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc []} {test-ddd [3:NsActive]}", orch.rost.TestString())
}

func TestJoinFailure_PrepareDropRange(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []} {test-ddd []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The left side of the join will not be released by aaa.
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.PrepareDropRange, fmt.Errorf("something went wrong"))

	// 0. Initiate

	_ = joinOp(orch, 1, 2, "test-ccc")

	// 1. PrepareAddRange

	tickWait(orch)
	require.Equal(t, "Give(R3, test-ccc)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, nodes.RPCs())
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	// 2. PrepareDropRange

	// The right side (R2 on test-bbb) succeeds, but the left fails and remains
	// in NsActive. Three attempts are made.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(orch)
		if attempt == 1 {
			require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", rpcsToString(nodes.RPCs()))
			require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
		} else {
			require.Equal(t, "Take(R1, test-aaa)", rpcsToString(nodes.RPCs()))
			require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
		}

		p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
		require.Equal(t, attempt, p.Attempts)
		require.False(t, p.GiveUpOnDeactivate)
	}

	// Gave up on R1, so reactivate the one which did deactivate.

	tickWait(orch)
	require.Equal(t, "Serve(R2, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
	require.True(t, mustGetPlacement(t, orch.ks, 1, "test-aaa").GiveUpOnDeactivate)

	// R2 updates state
	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	// R3 is wedged as inactive indefinitely...
	requireStable(t, orch)

	// ...until an operator forcibly drops it.
	nodes.Get("test-aaa").ForceDrop(1)

	// The orchestrator won't notice this by itself, because no RPCs are being
	// exchanged, because it has given up on asking aaa to deactivate R1. But
	// eventually the roster will send a probe and notice it's gone.
	orch.rost.Tick()
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	// Recovery: Now that R1 is gone, R2 can deactivate (again) and this time
	// there'll be no wedged parent to stop R3 from activating.

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsGiveUp} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsDropped} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Take(R2, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Serve(R3, test-ccc)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Equal(t, "Drop(R2, test-bbb)", rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsJoining} {2 (ggg, +inf] RsJoining} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(orch)
	require.Empty(t, rpcsToString(nodes.RPCs()))
	require.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	// This time we're done for real.
	requireStable(t, orch)
}

func TestJoinFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The new/child range will not activate on ccc.
	nodes.Get("test-ccc").SetReturnValue(t, 3, fake_node.AddRange, fmt.Errorf("failed AddRange (ccc, 1) for test"))

	joinOp(orch, 1, 2, "test-ccc")

	// The child range will be placed on ccc, but fail to activate a few times
	// and eventually give up.
	tickUntil(t, orch, nodes, func(ks, ro string) bool {
		return mustGetPlacement(t, orch.ks, 3, "test-ccc").GivenUpOnActivate
	})

	// This is a bad state to be in! But it's valid, because the parent ranges
	// must be deactivated before the child range can be activated.
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsInactive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// Couple of ticks later, the parent ranges are reactivated.
	tickWait(orch)
	tickWait(orch)
	require.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsActive} {2 (ggg, +inf] RsJoining p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}", orch.rost.TestString())

	// The above is arguably the end of the test, but fast forward to the stable
	// situation, which is that the placement which refused to activate (on ccc)
	// is eventually dropped, and a new placement is created and placed
	// somewhere else (aaa).
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [3:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

}

func TestJoinFailure_DropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()
	requireStable(t, orch)

	// The left side of the join will not be released by aaa.
	nodes.Get("test-aaa").SetReturnValue(t, 1, fake_node.DropRange, fmt.Errorf("failed DropRange for test"))

	joinOp(orch, 1, 2, "test-ccc")

	// End up in a basically fine state, with the joined range active on ccc,
	// the right (non-stuck) side of the parent dropped, and the left (stuck)
	// side inactive but still hanging around on aaa.
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, ggg] RsJoining p0=test-aaa:PsInactive} {2 (ggg, +inf] RsJoining} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.DropFailed)
}

func TestMissingPlacement(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	defer nodes.Close()

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

	// From here we continue as usual. No need to repeat TestPlace.

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
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
}

func TestSlowRPC(t *testing.T) {

	// One node, one range, unplaced.

	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, nodes := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
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
	par := nodes.Get("test-aaa").AddBarrier(t, 1, state.NsLoading)
	orch.Tick()
	par.Wait()

	// TODO: Use this pattern most places; just check the type and meta.
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
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())

	// Subsequent ticks continue the placement as usual. No need to verify the
	// details in this test.
	tickUntilStable(t, orch, nodes)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
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

	// {aa} {bb}
	r1 := regexp.MustCompile(`{[^{}]*}`)

	// {1 [-inf, ggg] RsActive p0=test-aaa:PsActive p1=test-bbb:PsActive} {2 (ggg, +inf] RsActive}
	r2 := regexp.MustCompile(`^{` + `(\d+)` + ` ` + `[\[\(]` + `([\+\-\w]+)` + `, ` + `([\+\-\w]+)` + `[\]\)]` + ` ` + `(Rs\w+)` + `(?: (.+))?` + `}$`)

	// p0=test-aaa:PsActive
	r3 := regexp.MustCompile(`^` + `p(\d+)` + `=` + `(.+)` + `:` + `(Ps\w+)` + `$`)

	x := r1.FindAllString(keyspace, -1)
	sr := make([]rangeStub, len(x))
	for i := range x {
		y := r2.FindStringSubmatch(x[i])
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
				z := r3.FindStringSubmatch(pl[ii])
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

func parseRoster(t *testing.T, s string) []nodeStub {

	// {aa} {bb}
	r1 := regexp.MustCompile(`{[^{}]*}`)

	// {test-aaa [1:NsActive 2:NsActive]}
	r2 := regexp.MustCompile(`^{` + `([\w\-]+)` + ` ` + `\[(.*)\]}$`)

	// 1:NsActive
	r3 := regexp.MustCompile(`^` + `(\d+)` + `:` + `(Ns\w+)` + `$`)

	x := r1.FindAllString(s, -1)
	ns := make([]nodeStub, len(x))
	for i := range x {
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
				State:  placementStateFromString(t, pstub.pState),
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

func rosterFactory(t *testing.T, cfg config.Config, ctx context.Context, ks *keyspace.Keyspace, stubs []nodeStub) (*fake_nodes.TestNodes, *roster.Roster) {
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
				State: remoteStateFromString(t, pStub.nState),
			}
		}

		nodes.Add(ctx, rem, ri)
	}

	rost.NodeConnFactory = nodes.NodeConnFactory
	return nodes, rost
}

// TODO: Merge this with orchFactoryCheck once TestJunk is gone.
func orchFactoryNoCheck(t *testing.T, sKS, sRos string, cfg config.Config, strict bool) (*Orchestrator, *fake_nodes.TestNodes) {
	ks := keyspaceFactory(t, cfg, parseKeyspace(t, sKS))
	nodes, ros := rosterFactory(t, cfg, context.TODO(), ks, parseRoster(t, sRos))
	nodes.SetStrictTransitions(strict)
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	orch := New(cfg, ks, ros, srv)
	return orch, nodes
}

func orchFactory(t *testing.T, sKS, sRos string, cfg config.Config, strict bool) (*Orchestrator, *fake_nodes.TestNodes) {
	orch, nodes := orchFactoryNoCheck(t, sKS, sRos, cfg, strict)

	// Tick once, to populate the roster before the first orchestrator tick. We
	// also do this when starting up the controller is starting up, so it's not
	// too much of a hack. (Doesn't happen before most ticks, though.)
	orch.rost.Tick()

	// Verify that the current state of the keyspace and roster is what was
	// requested. (Require it, because if not, the test harness is broken.)
	// TODO: Call nodes.Close() when these fail, somehow.
	require.Equal(t, sKS, orch.ks.LogString())
	require.Equal(t, sRos, orch.rost.TestString())

	return orch, nodes
}

func placementStateFromString(t *testing.T, s string) ranje.PlacementState {
	switch s {
	case ranje.PsUnknown.String():
		return ranje.PsUnknown

	case ranje.PsPending.String():
		return ranje.PsPending

	case ranje.PsInactive.String():
		return ranje.PsInactive

	case ranje.PsActive.String():
		return ranje.PsActive

	case ranje.PsGiveUp.String():
		return ranje.PsGiveUp

	case ranje.PsDropped.String():
		return ranje.PsDropped
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return ranje.PsUnknown // unreachable
}

func remoteStateFromString(t *testing.T, s string) state.RemoteState {
	switch s {
	case "NsUnknown":
		return state.NsUnknown
	case "NsLoading":
		return state.NsLoading
	case "NsInactive":
		return state.NsInactive
	case "NsActivating":
		return state.NsActivating
	case "NsActive":
		return state.NsActive
	case "NsDeactivating":
		return state.NsDeactivating
	case "NsDropping":
		return state.NsDropping
	case "NsNotFound":
		return state.NsNotFound
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return state.NsUnknown // unreachable
}

func splitOp(orch *Orchestrator, rID int) chan error {
	ch := make(chan error)
	rID_ := ranje.Ident(rID)

	op := OpSplit{
		Range: rID_,
		Key:   "ccc",
		Err:   ch,
	}

	orch.opSplitsMu.Lock()
	orch.opSplits[ranje.Ident(rID)] = op
	orch.opSplitsMu.Unlock()

	return ch
}

// JoinOp injects a join operation to the given orchestrator, to kick off the
// operation at the start of a test.
func joinOp(orch *Orchestrator, r1ID, r2ID int, dest string) chan error {
	ch := make(chan error)

	// TODO: Do this via the operator interface instead.

	// TODO: Inject the target node for r3. It currently defaults to the empty
	//       node

	op := OpJoin{
		Left:  ranje.Ident(r1ID),
		Right: ranje.Ident(r2ID),
		Dest:  dest,
		Err:   ch,
	}

	orch.opJoinsMu.Lock()
	orch.opJoins = append(orch.opJoins, op)
	orch.opJoinsMu.Unlock()

	return ch
}

// --------------------------------------------------------------------- helpers

type Waiter interface {
	Wait()
}

// tickWait performs a Tick, then waits for any pending RPCs to complete, then
// waits for any give Waiters (which are probably fake_node.Barrier instances)
// to return. If you've called AddBarrier on any nodes since the previous tick,
// you almost certainly want to wait for the barrier to be reached, by calling
// Wait.
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

	log.Print("ks: ", orch.ks.LogString())
	log.Print("ro: ", orch.rost.TestString())
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

// Tick repeatedly until the given callback (which is called with the string
// representation of the keyspace and roster after each tick) returns true.
func tickUntil(t *testing.T, orch *Orchestrator, nodes *fake_nodes.TestNodes, callback func(string, string) bool) {
	var ticks int // total ticks waited

	for {
		tickWait(orch)
		if callback(orch.ks.LogString(), orch.rost.TestString()) {
			break
		}

		ticks += 1
		if ticks > 50 {
			t.Fatal("gave up after 50 ticks")
			return
		}
	}

	// Call and discard, to omit RPCs sent during this loop from asserts.
	nodes.RPCs()
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

type rpcTuple struct {
	rID uint64 // sort by this
	nID string // and then this
	op  string
}

func (t *rpcTuple) String() string {
	return fmt.Sprintf("%s(R%d, %s)", t.op, t.rID, t.nID)
}

func (t *rpcTuple) Less(other rpcTuple) bool {
	if t.rID != other.rID {
		return t.rID < other.rID
	} else {
		return t.nID < other.nID
	}
}

func rpcsToString(input map[string][]interface{}) string {
	rpcs := []rpcTuple{}

	// Collect a tuple for every RPC.
	for nID := range input {
		for _, rpc := range input[nID] {
			tup := rpcToTuple(nID, rpc)
			rpcs = append(rpcs, tup)
		}
	}

	// Sort them into constant order.
	sort.Slice(rpcs, func(i, j int) bool {
		return rpcs[i].Less(rpcs[j])
	})

	// Cast to strings.
	strs := make([]string, len(rpcs))
	for i := range rpcs {
		strs[i] = rpcs[i].String()
	}

	// Return a single string.
	return strings.Join(strs, ", ")
}

func rpcToTuple(nID string, rpc interface{}) rpcTuple {
	tup := rpcTuple{
		nID: nID,
	}

	switch v := rpc.(type) {
	case *pb.GiveRequest:
		tup.rID = v.Range.Ident
		tup.op = "Give"

	case *pb.TakeRequest:
		tup.rID = v.Range
		tup.op = "Take"

	case *pb.ServeRequest:
		tup.rID = v.Range
		tup.op = "Serve"

	case *pb.DropRequest:
		tup.rID = v.Range
		tup.op = "Drop"

	default:
		// TODO: pb.InfoRequest
		// TODO: pb.RangesRequest
		panic(fmt.Sprintf("unknown RPC type: %T", v))
	}

	return tup
}

// mustGetRange returns a range from the given keyspace or fails the test.
func mustGetRange(t *testing.T, ks *keyspace.Keyspace, rID int) *ranje.Range {
	r, err := ks.Get(ranje.Ident(rID))
	if err != nil {
		t.Fatalf("ks.Get(%d): %v", rID, err)
	}
	return r
}

func mustGetPlacement(t *testing.T, ks *keyspace.Keyspace, rID int, nodeID string) *ranje.Placement {
	r := mustGetRange(t, ks, rID)
	p := r.PlacementByNodeID(nodeID)
	if p == nil {
		t.Fatalf("r(%d).PlacementByNodeID(%s): no such placement", rID, nodeID)
	}
	return p
}

// assertClosed asserts that the given error channel is closed.
func assertClosed(t *testing.T, ch <-chan error) {
	select {
	case err, ok := <-ch:
		if ok {
			assert.NoError(t, err)
		}
	default:
		assert.Fail(t, "expected channel to be closed")
	}
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
