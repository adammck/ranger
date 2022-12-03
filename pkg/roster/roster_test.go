package roster

import (
	"context"
	"fmt"
	"testing"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/test/fake_nodes"
	"github.com/stretchr/testify/suite"
)

// TODO: Remove this suite, just use funcs.
type RosterSuite struct {
	suite.Suite
	ctx   context.Context
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

	// Just to avoid constructing this thing everywhere.
	ts.r = &ranje.Range{
		Meta:  api.Meta{Ident: 1},
		State: api.RsActive,
	}

	// Empty by default.
	ts.nodes = fake_nodes.NewTestNodes()
}

func (ts *RosterSuite) Init() {
	ts.rost = New(ts.nodes.Discovery(), nil, nil, nil)
	ts.rost.NodeConnFactory = ts.nodes.NodeConnFactory
}

func (ts *RosterSuite) TestNoCandidates() {
	ts.Init()

	nID, err := ts.rost.Candidate(ts.r, ranje.AnyNode)
	if ts.Error(err) {
		ts.Equal(fmt.Errorf("no candidates available (rID=1, c=Constraint{any})"), err)
	}
	ts.Equal(nID, api.ZeroNodeID)
}

func (ts *RosterSuite) TestCandidateByNodeID() {

	aRem := api.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	bRem := api.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}

	cRem := api.Remote{
		Ident: "test-ccc",
		Host:  "host-ccc",
		Port:  1,
	}

	r := &ranje.Range{
		Meta: api.Meta{
			Ident: 1,
		},
		State: api.RsActive,
		Placements: []*ranje.Placement{{
			NodeID:       aRem.NodeID(),
			StateCurrent: api.PsActive,
		}},
	}

	aInfos := map[api.RangeID]*api.RangeInfo{
		r.Meta.Ident: {
			Meta:  r.Meta,
			State: api.NsActive,
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
		ts.Equal(api.NodeID("test-bbb"), nID)
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

	rem := api.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}

	r := &ranje.Range{
		Meta: api.Meta{
			Ident: 1,
			Start: api.ZeroKey,
			End:   api.Key("ggg"),
		},
		State: api.RsActive,
		Placements: []*ranje.Placement{{
			NodeID:       rem.NodeID(),
			StateCurrent: api.PsActive,
		}},
	}

	fakeInfos := map[api.RangeID]*api.RangeInfo{
		r.Meta.Ident: {
			Meta:  r.Meta,
			State: api.NsActive,
			Info: api.LoadInfo{
				Keys: 123,
			},
		},
	}

	ts.nodes.Add(ts.ctx, rem, fakeInfos)
	ts.Init()

	ts.rost.Discover()

	// Far as the roster is concerned, this is a real node.
	rostNode, err := ts.rost.NodeByIdent("test-aaa")
	ts.Require().NoError(err)
	ts.Require().NotNil(rostNode)

	err = ts.rost.probeOne(ts.ctx, rostNode)
	if ts.NoError(err) {
		if rostInfo, ok := rostNode.Get(1); ts.True(ok) {

			// The "real" RangeInfo, which we got from the (fake) remote via
			// gRPC (via bufconn) through its rangelet should match the fake
			// RangeInfo above.
			ts.Equal(*fakeInfos[r.Meta.Ident], rostInfo)
		}
	}
}
