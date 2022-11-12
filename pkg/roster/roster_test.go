package roster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/stretchr/testify/suite"
)

type RosterSuite struct {
	suite.Suite
	ctx   context.Context
	cfg   config.Config
	nodes *fake_nodes.TestNodes
	rost  *Roster

	// Not important
	r *ranje.Range
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(RosterSuite))
}

func (ts *RosterSuite) SetupTest() {
	ts.ctx = context.Background()

	// Sensible defaults.
	ts.cfg = config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       1 * time.Hour, // never
		Replication:              1,
	}

	// Just to avoid constructing this thing everywhere.
	ts.r = &ranje.Range{
		Meta:  ranje.Meta{Ident: 1},
		State: ranje.RsActive,
	}

	// Empty by default.
	ts.nodes = fake_nodes.NewTestNodes()
}

func (ts *RosterSuite) Init() {
	ts.rost = New(ts.cfg, ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory
}

func (ts *RosterSuite) TestNoCandidates() {
	ts.Init()

	nID, err := ts.rost.Candidate(ts.r, ranje.AnyNode)
	if ts.Error(err) {
		ts.Equal(fmt.Errorf("no candidates available (rID=1, c=any)"), err)
	}
	ts.Equal(nID, "")
}

func (ts *RosterSuite) TestCandidateByNodeID() {

	aRem := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	bRem := discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}

	cRem := discovery.Remote{
		Ident: "test-ccc",
		Host:  "host-ccc",
		Port:  1,
	}

	r := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: aRem.Ident,
			State:  ranje.PsActive,
		}},
	}

	aInfos := map[ranje.Ident]*info.RangeInfo{
		r.Meta.Ident: {
			Meta:  r.Meta,
			State: state.NsActive,
		},
	}

	ts.nodes.Add(ts.ctx, aRem, aInfos)
	ts.nodes.Add(ts.ctx, bRem, nil)
	ts.nodes.Add(ts.ctx, cRem, nil)
	ts.nodes.Get("test-ccc").SetWantDrain(true)

	ts.Init()
	ts.rost.Tick()

	// -------------------------------------------------------------------------

	nID, err := ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-bbb"})
	if ts.NoError(err) {
		ts.Equal("test-bbb", nID)
	}

	// This one doesn't exist
	_, err = ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-ddd"})
	if ts.Error(err) {
		ts.Equal("no such node: test-ddd", err.Error())
	}

	// This one already has the range
	_, err = ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-aaa"})
	if ts.Error(err) {
		ts.Equal("node already has range: test-aaa", err.Error())
	}

	// This one doesn't want any more ranges, because it's drained.
	// Note that we only know that the node wants to be drained because of the
	// roster tick, above. If we just called SetWantDrain(true) and didn't tick,
	// the "remote" node would want drain, but the roster wouldn't know that.
	_, err = ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-ccc"})
	if ts.Error(err) {
		ts.Equal("node wants drain: test-ccc", err.Error())
	}
}

func (ts *RosterSuite) TestProbeOne() {

	rem := discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	r := &ranje.Range{
		Meta: ranje.Meta{
			Ident: 1,
			Start: ranje.ZeroKey,
			End:   ranje.Key("ggg"),
		},
		State: ranje.RsActive,
		Placements: []*ranje.Placement{{
			NodeID: rem.Ident,
			State:  ranje.PsActive,
		}},
	}

	fakeInfos := map[ranje.Ident]*info.RangeInfo{
		r.Meta.Ident: {
			Meta:  r.Meta,
			State: state.NsActive,
			Info: info.LoadInfo{
				Keys: 123,
			},
		},
	}

	ts.nodes.Add(ts.ctx, rem, fakeInfos)
	ts.Init()

	ts.rost.Discover()

	// Far as the roster is concerned, this is a real node.
	rostNode := ts.rost.NodeByIdent("test-aaa")
	ts.Require().NotNil(rostNode)

	err := ts.rost.probeOne(ts.ctx, rostNode)
	if ts.NoError(err) {
		if rostInfo, ok := rostNode.Get(1); ts.True(ok) {

			// The "real" RangeInfo, which we got from the (fake) remote via
			// gRPC (via bufconn) through its rangelet should match the fake
			// RangeInfo above.
			ts.Equal(*fakeInfos[r.Meta.Ident], rostInfo)
		}
	}
}
