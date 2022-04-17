package roster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	"github.com/adammck/ranger/pkg/ranje"
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

	nID, err := ts.rost.Candidate(ts.r, *ranje.AnyNode())
	if ts.Error(err) {
		ts.Equal(fmt.Errorf("no candidates available (rID=1, c=any)"), err)
	}
	ts.Equal(nID, "")
}

func (ts *RosterSuite) TestCandidateByNodeID() {
	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-aaa",
		Host:  "host-aaa",
		Port:  1,
	}, nil)

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-bbb",
		Host:  "host-bbb",
		Port:  1,
	}, nil)

	ts.nodes.Add(ts.ctx, discovery.Remote{
		Ident: "test-ccc",
		Host:  "host-ccc",
		Port:  1,
	}, nil)

	ts.Init()
	ts.rost.Tick()

	nID, err := ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-bbb"})
	if ts.NoError(err) {
		ts.Equal(nID, "test-bbb")
	}

	nID, err = ts.rost.Candidate(ts.r, ranje.Constraint{NodeID: "test-ccc"})
	if ts.NoError(err) {
		ts.Equal(nID, "test-ccc")
	}
}
