package orchestrator

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"context"

	mock_actuator "github.com/adammck/ranger/pkg/actuator/mock"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	mock_disc "github.com/adammck/ranger/pkg/discovery/mock"
	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

func TestPlace(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	// First tick: Placement created, Give RPC sent to node and returned
	// successfully. Remote state is updated in roster, but not keyspace.

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())

	// Second tick: Keyspace is updated with state from roster. No RPCs sent.

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	// Third: Serve RPC is sent, to advance to ready. Returns success, and
	// roster is updated. Keyspace is not.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	// Forth: Keyspace is updated with ready state from roster. No RPCs sent.

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	// No more changes. This is steady state.

	requireStable(t, orch, act)
}

func TestPlace_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
}

func TestPlace_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)

	i1g := act.Inject("test-aaa", 1, api.Give).Response(api.NsLoading)

	//
	// ---- Give
	//

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsLoading]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsLoading]}", orch.rost.TestString())

	requireStable(t, orch, act)
	i1g.Response(api.NsInactive)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())

	// This tick notices that the remote state (which was updated at the end of
	// the previous tick, after the (redundant) Give RPC returned) now indicates
	// that the node has finished preparing.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())

	//
	// ---- Serve
	//

	i1s := act.Inject("test-aaa", 1, api.Serve).Response(api.NsActivating)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActivating]}", orch.rost.TestString())

	requireStable(t, orch, act)
	i1s.Response(api.NsActive)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch, act)
}

func TestPlaceFailure_PrepareAddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	act.Inject("test-aaa", 1, api.Give).Failure()

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

// TODO: Maybe remove this test since we have the long version below?
func TestPlaceFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	// Serving R1 will always fail on node aaa.
	// (But Give will succeed, as is the default)
	act.Inject("test-aaa", 1, api.Serve).Failure()

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestPlaceFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive}"
	rosStr := "{test-aaa []} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	// Serving R1 will always fail on node aaa.
	// (But Give will succeed, as is the default)
	act.Inject("test-aaa", 1, api.Serve).Failure()

	// 1. PrepareAddRange(1, aaa)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())

	// 2. AddRange(1, aaa)
	//    Makes three attempts.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
		require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []}", orch.rost.TestString())
	}

	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedActivate)

	// 3. DropRange(1, aaa)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []}", orch.rost.TestString())

	// 4. PrepareAddRange(1, bbb)
	// 5. AddRange(1, bbb)

	tickUntilStable(t, orch, act)
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestMove(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	requireStable(t, orch, act)

	moveOp(orch, 1, "test-bbb")

	tickWait(t, orch, act)
	assert.Equal(t, "Give(R1, test-bbb)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Just updates state from roster.
	// TODO: As above, should maybe trigger the next tick automatically.
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-bbb)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// p0 is gone!
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch, act)
}

func TestMove_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	requireStable(t, orch, act)

	moveOp(orch, 1, "test-bbb")
	tickUntilStable(t, orch, act)

	// Range moved from aaa to bbb.
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
}

func TestMove_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	moveOp(orch, 1, "test-bbb")

	//
	// ---- Give
	//

	// Next Give will return NsLoading because it's "slow".
	act.Inject("test-bbb", 1, api.Give).Response(api.NsLoading)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsLoading]}", orch.rost.TestString())

	// We can do this all day.
	requireStable(t, orch, act)

	// Loading finished. The next Give will return Inactive.
	act.Inject("test-bbb", 1, api.Give).Response(api.NsInactive)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-bbb)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Updates placement from roster.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	//
	// ---- Take
	//

	// Next Take will return NsDeactivating because it's "slow".
	act.Inject("test-aaa", 1, api.Take).Response(api.NsDeactivating)

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// We can do this all day.
	requireStable(t, orch, act)

	// Deactivation finished. The next Take will return Inactive.
	act.Inject("test-aaa", 1, api.Take).Response(api.NsInactive)

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// TODO: Missing tick!
	// Updates placement from roster?

	//
	// ---- Serve
	//

	// Next Serve will return NsActivating because it's "slow".
	act.Inject("test-bbb", 1, api.Serve).Response(api.NsActivating)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActivating]}", orch.rost.TestString())

	// We can do this all day.
	requireStable(t, orch, act)

	// Activation finished. The next Serve will return Active.
	act.Inject("test-bbb", 1, api.Serve).Response(api.NsActive)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-bbb)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// Updates placement from roster.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	//
	// ---- Drop
	//

	// Next Drop will return NsDropping because it's "slow".
	act.Inject("test-aaa", 1, api.Drop).Response(api.NsDropping)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// We can do this all day.
	requireStable(t, orch, act)

	// Deactivation finished. The next Take will return Inactive.
	act.Inject("test-aaa", 1, api.Drop).Response(api.NsNotFound)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// Updates placement from roster.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	//
	// ---- Cleanup
	//

	// test-aaa is gone!
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	// IsReplacing annotation is gone.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())

	requireStable(t, orch, act)
}

func TestMoveFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-bbb", 1, api.Give).Failure()
	moveOp(orch, 1, "test-bbb")

	// Make three attempts at giving R1 to bbb, which all fail.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		require.Equal(t, "Give(R1, test-bbb)", act.Commands())
		require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())
	}

	p := mustGetPlacement(t, orch.ks, 1, "test-bbb")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedGive)

	// Failed placement is destroyed.

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Done.

	requireStable(t, orch, act)
}

func TestMoveFailure_PrepareDropRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-aaa", 1, api.Take).Failure()
	moveOp(orch, 1, "test-bbb")

	// 1. Node B gets PrepareAddRange to verify that it can take the shard. This
	//    succeeds (because nothing has failed yet).

	tickWait(t, orch, act)
	assert.Equal(t, "Give(R1, test-bbb)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// Keyspace updates from roster.
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 2. Node A gets PrepareDropRange, which fails because we injected an error
	//    above. This repeats three times before we give up and accept that the
	//    node will not relinquish the range.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		assert.Equal(t, "Take(R1, test-aaa)", act.Commands())
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())
	}

	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedDeactivate)

	// 3. Node B gets DropRange, to abandon the placement it prepared. It will
	//    never become ready.

	tickWait(t, orch, act)
	assert.Equal(t, "Drop(R1, test-bbb)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Destroy the abandonned placement.
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsDropped:replacing(test-aaa)}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Destroy the abandonned placement.
	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	// Stable now, because the move was a one-off. (The balancer might try the
	// same thing again, but that's a separate test.)
	requireStable(t, orch, act)
}

func TestMoveFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-bbb", 1, api.Serve).Failure()
	moveOp(orch, 1, "test-bbb")

	tickWait(t, orch, act)
	require.Equal(t, "Give(R1, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsPending:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// TODO: Why doesn't this step happen? Think it's the placement-ordering
	//       thing where some stuff happens in one step instead of two if
	//       dependent placements are ordered a certain way. Need to perform two
	//       separate steps: update keyspace, then send RPCs.

	// tickWait(t, orch, act)
	// require.Empty(t, act.Commands())
	// require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	// require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 3. AddRange(1, bbb)
	//    Makes three attempts, which will all fail because we stubbed them to
	//    to do so, above.
	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		require.Equal(t, "Serve(R1, test-bbb)", act.Commands())
		require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())
	}

	p := mustGetPlacement(t, orch.ks, 1, "test-bbb")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedActivate)

	// Range is reactivated on the original node, because the new one failed.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// TODO: This step is also merged with the next :|
	// tickWait(t, orch, act)
	// require.Empty(t, act.Commands())
	// require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	// require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [1:NsInactive]}", orch.rost.TestString())

	// 5. DropRange(1, bbb)
	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive p1=test-bbb:PsInactive:replacing(test-aaa)}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []}", orch.rost.TestString())

	requireStable(t, orch, act)
}

func TestMoveFailure_DropRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	moveOp(orch, 1, "test-bbb")

	i1d := act.Inject("test-aaa", 1, api.Drop).Failure()

	// Fast-forward to the part where we send DropRange to aaa.
	tickUntil(t, orch, act, func(ks, ro string) bool {
		return (true &&
			ks == "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}" &&
			ro == "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}")
	})

	for attempt := 1; attempt <= 5; attempt++ {
		tickWait(t, orch, act)

		// The RPC was sent.
		assert.Equal(t, "Drop(R1, test-aaa)", act.Commands())

		// But no state changed.
		assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsInactive p1=test-bbb:PsActive:replacing(test-aaa)}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [1:NsActive]}", orch.rost.TestString())

		// (Except for this counter.)
		assert.Equal(t, attempt, mustGetPlacement(t, orch.ks, 1, "test-aaa").DropFailures)
	}

	// Not checking stability here. Failing to drop will retry forever until an
	// operator intervenes to force the node to drop the placement. This hack
	// pretends that that happened, so we can observe the workflow unblocking.
	i1d.Success().Response(api.NsNotFound)

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [1:NsActive]}", orch.rost.TestString())
}

func TestSplit(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	requireStable(t, orch, act)

	// 0. Initiate

	opErr := splitOp(orch, 1)

	// 1. PrepareAddRange

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))

	tickWait(t, orch, act)
	assert.Equal(t, "Give(R2, test-aaa), Give(R3, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// 2. PrepareDropRange

	tickWait(t, orch, act)
	assert.Equal(t, "Take(R1, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// 3. AddRange

	tickWait(t, orch, act)
	assert.Equal(t, "Serve(R2, test-aaa), Serve(R3, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())

	// 4. DropRange

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())

	// 5. Cleanup

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks)) // Operation is still active.

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Empty(t, OpsString(orch.ks)) // Operation has finished.

	requireStable(t, orch, act)
	assertClosed(t, opErr)
}

func TestSplit_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	splitOp(orch, 1)
	tickUntilStable(t, orch, act)

	// Range 1 was split into ranges 2 and 3 at ccc.
	assert.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
}

func TestSplit_Slow(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	opErr := splitOp(orch, 1)

	//
	// ---- Init
	// Split initiated by controller. Node hasn't heard about it yet.
	//

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))

	//
	// ---- Give
	// Controller places new ranges on nodes.
	//

	i2g := act.Inject("test-aaa", 2, api.Give).Response(api.NsLoading)
	i3g := act.Inject("test-aaa", 3, api.Give).Response(api.NsLoading)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R2, test-aaa), Give(R3, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive 2:NsLoading 3:NsLoading]}", orch.rost.TestString())
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))

	requireStable(t, orch, act)
	i2g.Response(api.NsInactive) // R2 finished loading (but R3 is ongoing).

	tickWait(t, orch, act)
	require.Equal(t, "Give(R2, test-aaa), Give(R3, test-aaa)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsPending} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsLoading]}", orch.rost.TestString())
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))

	// Updates placement from roster.
	tickWait(t, orch, act)
	require.Equal(t, "Give(R3, test-aaa)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsLoading]}", orch.rost.TestString())

	requireStable(t, orch, act)
	i3g.Response(api.NsInactive) // R3 finished loading.

	tickWait(t, orch, act)
	require.Equal(t, "Give(R3, test-aaa)", act.Commands()) // retry
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	// Updates placement from roster.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	//
	// ---- Take
	// Controller takes placements in parent range.
	//

	i1t := act.Inject("test-aaa", 1, api.Take).Response(api.NsDeactivating)

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsDeactivating 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	requireStable(t, orch, act)
	i1t.Response(api.NsInactive) // R3 finished loading.

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsInactive 3:NsInactive]}", orch.rost.TestString())

	//
	// ---- Serve
	// Controller instructs both child ranges to become Ready.
	//

	i2s := act.Inject("test-aaa", 2, api.Serve).Response(api.NsActivating)
	i3s := act.Inject("test-aaa", 3, api.Serve).Response(api.NsActivating)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R2, test-aaa), Serve(R3, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActivating]}", orch.rost.TestString())

	requireStable(t, orch, act)
	i3s.Response(api.NsActive) // R3 activated.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R2, test-aaa), Serve(R3, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsInactive}", orch.ks.LogString())

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R2, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsActivating 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	requireStable(t, orch, act)
	i2s.Response(api.NsActive) // R2 activated.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R2, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsInactive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [1:NsInactive 2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	//
	// ---- Drop
	// Orchestrator instructs parent range to drop placements.
	//

	i1d := act.Inject("test-aaa", 1, api.Drop).Response(api.NsDropping)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsDropping 2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [1:NsDropping 2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	requireStable(t, orch, act)
	i1d.Response(api.NsNotFound) // R1 finished dropping.

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa)", act.Commands())
	require.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsDropped} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	//
	// ---- Cleanup
	//

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{test-aaa [2:NsActive 3:NsActive]}", orch.rost.TestString())
	require.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-aaa:PsActive} {3 (ccc, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())

	requireStable(t, orch, act)
	assertClosed(t, opErr)
}

func TestSplitFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-bbb", 2, api.Give).Failure()
	splitOp(orch, 1)

	// 1. PrepareAddRange

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))
	require.Equal(t, "Give(R2, test-bbb), Give(R3, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	for attempt := 2; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		// Only the failing placement (rID=2) will be retried.
		require.Equal(t, "Give(R2, test-bbb)", act.Commands())
		assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
		assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	}

	p := mustGetPlacement(t, orch.ks, 2, "test-bbb")
	assert.Equal(t, 3, p.Failures)
	assert.True(t, p.FailedGive)

	tickWait(t, orch, act)
	// Failed placement is destroyed.
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 2. PrepareAddRange (retry on ccc)

	tickWait(t, orch, act)
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))
	require.Equal(t, "Give(R2, test-ccc)", act.Commands())

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-ccc:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc [2:NsInactive]}", orch.rost.TestString())

	// Recovered! Finish the split.

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_PrepareDropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-aaa", 1, api.Take).Failure()
	splitOp(orch, 1)

	// End up in a bad but stable situation where the original range never
	// relinquish (that's the point), but that the successors don't activate.
	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// R1 is stuck until some operator comes and unsticks it.
	// TODO: Make it possible (configurable) to automatically force drop it.
	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.FailedDeactivate)
}

func TestSplitFailure_PrepareDropRange(t *testing.T) {
	t.Skip("not implemented")
}

func TestSplitFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-bbb", 2, api.Serve).Failure()
	splitOp(orch, 1)

	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_AddRange(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-bbb", 2, api.Serve).Failure()
	splitOp(orch, 1)

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

	// 1. PrepareAddRange

	tickWait(t, orch, act)
	require.Equal(t, "Give(R2, test-bbb), Give(R3, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 2. PrepareDropRange

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())

	// 3. AddRange
	// Three attempts. The first one goes to both sides of the split, succeeds
	// on the right side (R3), but fails on the left (R2). The next two attempts
	// only go to the failed side, and it fails twice more.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		if attempt == 1 {
			require.Equal(t, "Serve(R2, test-bbb), Serve(R3, test-bbb)", act.Commands())
			require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsActive]} {test-ccc []}", orch.rost.TestString())
		} else {
			require.Equal(t, "Serve(R2, test-bbb)", act.Commands())
			require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsActive]} {test-ccc []}", orch.rost.TestString())
		}
	}

	p := mustGetPlacement(t, orch.ks, 2, "test-bbb")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedActivate)

	// 4. PrepareDropRange
	//
	// Undo the Serve that succeeded, so we can reactivate the predecessor while
	// a new placement is found for the Serve that failed. Give can be slow, but
	// Take and Serve should be fast.

	tickWait(t, orch, act)
	require.Equal(t, "Take(R3, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.True(t, mustGetPlacement(t, orch.ks, 2, "test-bbb").FailedActivate)
	require.Equal(t, "{Split 1 <- 2,3}", OpsString(orch.ks))

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.Equal(t, "{Split 1 <- 2,3}", OpsString(orch.ks))

	// 5. AddRange
	//
	// The parent (R1) is now reactivated, so it can be active while the failed
	// child is replaced.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R1, test-aaa)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive 3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.Equal(t, "{Split 1 <- 2,3}", OpsString(orch.ks))

	// 6. DropRange
	// The failed child is dropped.

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-bbb:PsInactive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.Equal(t, "{Split 1 <- 2,3}", OpsString(orch.ks))

	// Now the situation is basically stable, and the operation is inverted back
	// to the normal/forwards direction so we can continue placing the split.
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc []}", orch.rost.TestString())
	require.Equal(t, "{Split 1 -> 2,3}", OpsString(orch.ks))

	// 7. PrepareAddRange (retry)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R2, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsActive} {2 [-inf, ccc] RsActive p0=test-ccc:PsPending} {3 (ccc, +inf] RsActive p0=test-bbb:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [3:NsInactive]} {test-ccc [2:NsInactive]}", orch.rost.TestString())

	// Recovered! Let the re-placement of R3 on Nccc finish.

	tickUntilStable(t, orch, act)
	require.Equal(t, "{1 [-inf, +inf] RsObsolete} {2 [-inf, ccc] RsActive p0=test-ccc:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [3:NsActive]} {test-ccc [2:NsActive]}", orch.rost.TestString())
}

func TestSplitFailure_DropRange(t *testing.T) {
	t.Skip("not implemented")
}

func TestSplitFailure_DropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb []} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-aaa", 1, api.Drop).Failure()
	splitOp(orch, 1)

	// End up in a bad but stable situation where the original range never
	// relinquish (that's the point), but that the successors don't activate.
	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, +inf] RsSubsuming p0=test-aaa:PsInactive} {2 [-inf, ccc] RsActive p0=test-bbb:PsActive} {3 (ccc, +inf] RsActive p0=test-bbb:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsActive 3:NsActive]} {test-ccc []}", orch.rost.TestString())

	// R1 is stuck until some operator comes and unsticks it.
	// TODO: Make it possible (configurable) to automatically force drop it.
	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.FailedDrop)
}

func TestJoin(t *testing.T) {
	t.Skip("not implemented")
}

func TestJoin_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	joinOp(orch, 1, 2, "test-ccc")

	tickUntilStable(t, orch, act)
	// Ranges 1 and 2 were joined into range 3, which holds the entire keyspace.
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())
}

func TestJoin_Slow(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), strictTransactions)
	opErr := joinOp(orch, 1, 2, "test-ccc")

	//
	// ---- Init
	// Controller initiates join.
	//

	//
	// ---- Give
	// Controller places new range on node.
	//

	i3g := act.Inject("test-ccc", 3, api.Give).Response(api.NsLoading)

	tickWait(t, orch, act)
	require.Equal(t, "Give(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsLoading]}", orch.rost.TestString())
	require.Equal(t, "{Join 1,2 -> 3}", OpsString(orch.ks))

	requireStable(t, orch, act)
	i3g.Response(api.NsInactive) // R3 finished loading.

	tickWait(t, orch, act)
	require.Equal(t, "Give(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())
	require.Equal(t, "{Join 1,2 -> 3}", OpsString(orch.ks))

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())
	require.Equal(t, "{Join 1,2 -> 3}", OpsString(orch.ks))

	//
	// ---- Take
	// Controller takes the ranges from the source nodes.
	//

	i1g := act.Inject("test-aaa", 1, api.Take).Response(api.NsDeactivating)
	i2g := act.Inject("test-bbb", 2, api.Take).Response(api.NsDeactivating)

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsDeactivating]} {test-bbb [2:NsDeactivating]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// Parent ranges finish deactivating.
	requireStable(t, orch, act)
	i1g.Response(api.NsInactive)
	i2g.Response(api.NsInactive)

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// Missing?
	// tickWait(t, orch, act)
	// require.Empty(t, act.Commands())
	// require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", act.Commands())
	// require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	// require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	//
	// ---- Serve
	// Controller instructs child range to become Ready.
	//

	i3s := act.Inject("test-ccc", 3, api.Serve).Response(api.NsActivating)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActivating]}", orch.rost.TestString())

	// New range becomes ready.
	requireStable(t, orch, act)
	i3s.Response(api.NsActive)

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]}", orch.rost.TestString())

	//
	// ---- Drop
	// Orchestrator instructs parent ranges to drop placements.
	//

	i1d := act.Inject("test-aaa", 1, api.Drop).Response(api.NsDropping)
	i2d := act.Inject("test-bbb", 2, api.Drop).Response(api.NsDropping)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa), Drop(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsDropping]} {test-bbb [2:NsDropping]} {test-ccc [3:NsActive]}", orch.rost.TestString())

	// Drops finish.

	requireStable(t, orch, act)
	i1d.Response(api.NsNotFound)
	i2d.Response(api.NsNotFound)

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R1, test-aaa), Drop(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	//
	// ---- Cleanup
	//

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	requireStable(t, orch, act)
	assertClosed(t, opErr)
}

func TestJoinFailure_PrepareAddRange(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []} {test-ddd []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-ccc", 3, api.Give).Failure()

	// 0. Initiate

	_ = joinOp(orch, 1, 2, "test-ccc")

	// 1. PrepareAddRange
	// Makes three attempts.

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		assert.Equal(t, "Give(R3, test-ccc)", act.Commands())
		require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
		require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []} {test-ddd []}", orch.rost.TestString())
	}

	// Gave up on test-ccc...
	p := mustGetPlacement(t, orch.ks, 3, "test-ccc")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedGive)

	// But for better or worse, R3 now exists, and the orchestrator will try to
	// place it rather than giving up altogether and abandonning the range. I'm
	// not sure if that's right -- maybe it'd be better to just give up and set
	// the predecessors back to RsActive? -- but this way Just Works.

	tickUntilStable(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ddd:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc []} {test-ddd [3:NsActive]}", orch.rost.TestString())
}

func TestJoinFailure_PrepareDropRange(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []} {test-ddd []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	_ = joinOp(orch, 1, 2, "test-ccc")

	i1t := act.Inject("test-aaa", 1, api.Take).Failure()

	//
	// ---- Give
	//

	tickWait(t, orch, act)
	require.Equal(t, "Give(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsPending}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
	require.Equal(t, "{Join 1,2 -> 3}", OpsString(orch.ks))

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	//
	// ---- Take
	// The right side (R2 on test-bbb) succeeds, but the left fails and remains
	// in NsActive. Three attempts are made.
	//

	for attempt := 1; attempt <= 3; attempt++ {
		tickWait(t, orch, act)
		if attempt == 1 {
			require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", act.Commands())
			require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
		} else {
			require.Equal(t, "Take(R1, test-aaa)", act.Commands())
			require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
			require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
		}
	}

	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	require.Equal(t, 3, p.Failures)
	require.True(t, p.FailedDeactivate)

	// Gave up on R1, so reactivate the one which did deactivate.

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
	require.True(t, mustGetPlacement(t, orch.ks, 1, "test-aaa").FailedDeactivate)
	require.Equal(t, "{Join 1,2 <- 3}", OpsString(orch.ks))

	// R2 updates state
	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())

	// R3 is wedged as inactive indefinitely...
	requireStable(t, orch, act)

	// ...until an operator forcibly drops it.
	i1t.Success().Response(api.NsNotFound)

	// The orchestrator won't notice this by itself, because no RPCs are being
	// exchanged, because it has given up on asking aaa to deactivate R1. But
	// eventually the roster will send a probe and notice it's gone.
	//orch.rost.Tick()
	//require.Equal(t, "{test-aaa []} {test-bbb [2:NsActive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
	//
	// TODO: Fix this so the test can continue via probing. This is broken
	//       because actuation is now mocked, but the roster isn't, so tries to
	//       send its own actual rpc probes which of course return nonsense.

	// For now, reset the failure flag so the next tick will try again. (This
	// actually seems like something there should be an operator interface for!)
	r1p0 := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	r1p0.FailedDeactivate = false
	r1p0.Failures = 0

	tickWait(t, orch, act)
	require.Equal(t, "Take(R1, test-aaa), Take(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]} {test-ddd []}", orch.rost.TestString())
	require.Equal(t, "{Join 1,2 -> 3}", OpsString(orch.ks))

	//
	// ---- Serve
	//

	tickWait(t, orch, act)
	require.Equal(t, "Serve(R3, test-ccc)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsGiveUp} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsDropped} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb [2:NsInactive]} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	//
	// ---- Drop
	//

	tickWait(t, orch, act)
	require.Equal(t, "Drop(R2, test-bbb)", act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsDropped} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	//
	// ---- Cleanup
	//

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	tickWait(t, orch, act)
	require.Empty(t, act.Commands())
	require.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa []} {test-bbb []} {test-ccc [3:NsActive]} {test-ddd []}", orch.rost.TestString())

	// This time we're done for real.
	requireStable(t, orch, act)
}

func TestJoinFailure_AddRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-ccc", 3, api.Serve).Failure()
	joinOp(orch, 1, 2, "test-ccc")

	// The child range will be placed on ccc, but fail to activate a few times
	// and eventually give up.
	tickUntil(t, orch, act, func(ks, ro string) bool {
		return mustGetPlacement(t, orch.ks, 3, "test-ccc").FailedActivate
	})

	// This is a bad state to be in! But it's valid, because the parent ranges
	// must be deactivated before the child range can be activated.
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsInactive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb [2:NsInactive]} {test-ccc [3:NsInactive]}", orch.rost.TestString())

	// Couple of ticks later, the parent ranges are reactivated.
	tickWait(t, orch, act)
	tickWait(t, orch, act)
	require.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsActive} {2 (ggg, +inf] RsSubsuming p0=test-bbb:PsActive} {3 [-inf, +inf] RsActive p0=test-ccc:PsInactive}", orch.ks.LogString())
	require.Equal(t, "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}", orch.rost.TestString())

	// The above is arguably the end of the test, but fast forward to the stable
	// situation, which is that the placement which refused to activate (on ccc)
	// is eventually dropped, and a new placement is created and placed
	// somewhere else (aaa).
	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, ggg] RsObsolete} {2 (ggg, +inf] RsObsolete} {3 [-inf, +inf] RsActive p0=test-aaa:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [3:NsActive]} {test-bbb []} {test-ccc []}", orch.rost.TestString())

}

func TestJoinFailure_DropRange_Short(t *testing.T) {
	ksStr := "{1 [-inf, ggg] RsActive p0=test-aaa:PsActive} {2 (ggg, +inf] RsActive p0=test-bbb:PsActive}"
	rosStr := "{test-aaa [1:NsActive]} {test-bbb [2:NsActive]} {test-ccc []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)
	act.Inject("test-aaa", 1, api.Drop).Failure()
	joinOp(orch, 1, 2, "test-ccc")

	// End up in a basically fine state, with the joined range active on ccc,
	// the right (non-stuck) side of the parent dropped, and the left (stuck)
	// side inactive but still hanging around on aaa.
	tickUntilStable(t, orch, act)
	assert.Equal(t, "{1 [-inf, ggg] RsSubsuming p0=test-aaa:PsInactive} {2 (ggg, +inf] RsSubsuming} {3 [-inf, +inf] RsActive p0=test-ccc:PsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]} {test-bbb []} {test-ccc [3:NsActive]}", orch.rost.TestString())

	p := mustGetPlacement(t, orch.ks, 1, "test-aaa")
	assert.True(t, p.FailedDrop)
}

func TestMissingPlacement(t *testing.T) {
	ksStr := "{1 [-inf, +inf] RsActive p0=test-aaa:PsActive}"
	rosStr := "{test-aaa []}"
	orch, act := orchFactory(t, ksStr, rosStr, testConfig(), noStrictTransactions)

	// Orchestrator notices that the node doesn't have the range, so marks the
	// placement as abandoned.

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsGiveUp}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// Orchestrator advances to drop the placement, but (unlike when moving)
	// doesn't bother to notify the node via RPC. It has already told us that it
	// doesn't have the range.

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsDropped}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// The placement is destroyed.

	tickWait(t, orch, act)
	assert.Empty(t, act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa []}", orch.rost.TestString())

	// From here we continue as usual. No need to repeat TestPlace.

	tickWait(t, orch, act)
	assert.Equal(t, "Give(R1, test-aaa)", act.Commands())
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=test-aaa:PsPending}", orch.ks.LogString())
	assert.Equal(t, "{test-aaa [1:NsInactive]}", orch.rost.TestString())
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
		r := ranje.NewRange(api.Ident(i + 1))
		r.State = api.RsActive

		if i > 0 {
			r.Meta.Start = api.Key(stubs[i].startKey)
			ranges[i-1].Meta.End = api.Key(stubs[i].startKey)
		}

		r.Placements = make([]*ranje.Placement, len(stubs[i].placements))

		for ii := range stubs[i].placements {
			pstub := stubs[i].placements[ii]
			ps := placementStateFromString(t, pstub.pState)
			r.Placements[ii] = &ranje.Placement{
				NodeID:       pstub.nodeID,
				StateCurrent: ps,
				StateDesired: ps,
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

func rosterFactory(t *testing.T, cfg config.Config, ctx context.Context, ks *keyspace.Keyspace, stubs []nodeStub) *roster.Roster {
	disc := mock_disc.New()

	for i := range stubs {
		nID := stubs[i].nodeID
		disc.Add("node", discovery.Remote{
			Ident: nID,
			Host:  fmt.Sprintf("host-%s", nID),
			Port:  1,
		})
	}

	rost := roster.New(cfg, disc, nil, nil, nil)

	// Run a single discovery cycle to populate rost.Nodes with empty nodes from
	// the mock discovery above. But don't actually tick; that part isn't mocked
	// yet, so would send RPCs and get confused.
	rost.Discover()

	for i := range stubs {
		nID := stubs[i].nodeID
		nod := rost.Nodes[nID]

		for _, pStub := range stubs[i].placements {
			rID := api.Ident(pStub.rID)
			r, err := ks.Get(rID)
			if err != nil {
				t.Fatalf("invalid node placement stub: %v", err)
			}

			nod.UpdateRangeInfo(&api.RangeInfo{
				Meta:  r.Meta,
				State: remoteStateFromString(t, pStub.nState),
			})
		}
	}

	return rost
}

// TODO: Merge this with orchFactoryCheck once TestJunk is gone.
func orchFactoryNoCheck(t *testing.T, sKS, sRos string, cfg config.Config, strict bool) (*Orchestrator, *mock_actuator.Actuator) {
	ks := keyspaceFactory(t, cfg, parseKeyspace(t, sKS))
	ros := rosterFactory(t, cfg, context.TODO(), ks, parseRoster(t, sRos))
	srv := grpc.NewServer() // TODO: Allow this to be nil.
	act := mock_actuator.New(ks, ros, strict)
	orch := New(cfg, ks, ros, srv)
	return orch, act
}

func orchFactory(t *testing.T, sKS, sRos string, cfg config.Config, strict bool) (*Orchestrator, *mock_actuator.Actuator) {
	orch, act := orchFactoryNoCheck(t, sKS, sRos, cfg, strict)

	// Verify that the current state of the keyspace and roster is what was
	// requested. (Require it, because if not, the test harness is broken.)
	require.Equal(t, sKS, orch.ks.LogString())
	require.Equal(t, sRos, orch.rost.TestString())

	return orch, act
}

func placementStateFromString(t *testing.T, s string) api.PlacementState {
	switch s {
	case api.PsUnknown.String():
		return api.PsUnknown

	case api.PsPending.String():
		return api.PsPending

	case api.PsInactive.String():
		return api.PsInactive

	case api.PsActive.String():
		return api.PsActive

	case api.PsGiveUp.String():
		return api.PsGiveUp

	case api.PsDropped.String():
		return api.PsDropped
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return api.PsUnknown // unreachable
}

func remoteStateFromString(t *testing.T, s string) api.RemoteState {
	switch s {
	case "NsUnknown":
		return api.NsUnknown
	case "NsLoading":
		return api.NsLoading
	case "NsInactive":
		return api.NsInactive
	case "NsActivating":
		return api.NsActivating
	case "NsActive":
		return api.NsActive
	case "NsDeactivating":
		return api.NsDeactivating
	case "NsDropping":
		return api.NsDropping
	case "NsNotFound":
		return api.NsNotFound
	}

	t.Fatalf("invalid PlacementState string: %s", s)
	return api.NsUnknown // unreachable
}

func moveOp(orch *Orchestrator, rID int, dest string) chan error {
	ch := make(chan error)

	op := OpMove{
		Range: api.Ident(rID),
		Dest:  dest,
	}

	orch.opMovesMu.Lock()
	orch.opMoves = append(orch.opMoves, op)
	orch.opMovesMu.Unlock()

	return ch
}

func splitOp(orch *Orchestrator, rID int) chan error {
	ch := make(chan error)
	rID_ := api.Ident(rID)

	op := OpSplit{
		Range: rID_,
		Key:   "ccc",
		Err:   ch,
	}

	orch.opSplitsMu.Lock()
	orch.opSplits[rID_] = op
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
		Left:  api.Ident(r1ID),
		Right: api.Ident(r2ID),
		Dest:  dest,
		Err:   ch,
	}

	orch.opJoinsMu.Lock()
	orch.opJoins = append(orch.opJoins, op)
	orch.opJoinsMu.Unlock()

	return ch
}

// --------------------------------------------------------------------- helpers

func OpsString(ks *keyspace.Keyspace) string {
	ops, err := ks.Operations()
	if err != nil {
		// This should not happen in tests. Call ks.Operation directly if
		// expecting an error. This helper is to quickly inspect contents.
		return fmt.Sprintf("Keyspace.Operations returned error: %v", err)
	}

	strs := make([]string, len(ops))
	for i := range ops {
		strs[i] = ops[i].TestString()
	}

	// Return a single string.
	return strings.Join(strs, ", ")
}

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
//
// TODO: Get rid of this function, now that we have a mock actuator!
func tickWait(t *testing.T, orch *Orchestrator, act *mock_actuator.Actuator, waiters ...Waiter) {
	t.Helper()

	act.Reset()
	orch.Tick()
	act.Tick()
	act.Wait()

	if len(waiters) > 0 {
		log.Printf("waiting for %d waiters", len(waiters))
		for _, w := range waiters {
			w.Wait()
		}
	}

	log.Print("ks: ", orch.ks.LogString())
	log.Print("ro: ", orch.rost.TestString())
	log.Print("op: ", OpsString(orch.ks))

	// If any unexpected commands were sent during this tick (which is only the
	// case if strict actuations are enabled), fail.
	if u := act.Unexpected(); len(u) > 0 {
		s := make([]string, len(u))
		for i := 0; i < len(u); i++ {
			s[i] = u[i].String()
		}
		cmds := strings.Join(s, ", ")
		t.Fatalf("unexpected command(s) while strict actuation enabled: %s", cmds)
	}
}

func tickUntilStable(t *testing.T, orch *Orchestrator, act *mock_actuator.Actuator) {
	var ksPrev string // previous value of ks.LogString
	var stable int    // ticks since keyspace changed or rpc sent
	var ticks int     // total ticks waited

	for {
		tickWait(t, orch, act)
		ks := orch.ks.LogString()
		cmds := act.Commands()

		if ks != ksPrev || cmds != "" {
			ksPrev = ks
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
}

// Tick repeatedly until the given callback (which is called with the string
// representation of the keyspace and roster after each tick) returns true.
func tickUntil(t *testing.T, orch *Orchestrator, act *mock_actuator.Actuator, callback func(string, string) bool) {
	var ticks int // total ticks waited

	for {
		tickWait(t, orch, act)
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
	act.Commands()
}

func requireStable(t *testing.T, orch *Orchestrator, act *mock_actuator.Actuator) {
	t.Helper()

	ksLog := orch.ks.LogString()
	rostLog := orch.rost.TestString()
	for i := 0; i < 2; i++ {
		tickWait(t, orch, act)

		// Use require (vs assert) since spamming the same error doesn't help.
		require.Equal(t, ksLog, orch.ks.LogString())
		require.Equal(t, rostLog, orch.rost.TestString())
	}

	// Call and discard, to omit RPCs sent during this loop from asserts.
	act.Commands()
}

// mustGetRange returns a range from the given keyspace or fails the test.
func mustGetRange(t *testing.T, ks *keyspace.Keyspace, rID int) *ranje.Range {
	r, err := ks.Get(api.Ident(rID))
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
