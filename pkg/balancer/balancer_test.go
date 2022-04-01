package balancer

import (
	"testing"

	"github.com/adammck/ranger/pkg/config"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type FakePersister struct {
}

func (fp *FakePersister) GetRanges() ([]*ranje.Range, error) {
	return []*ranje.Range{}, nil
}

func (fp *FakePersister) PutRanges([]*ranje.Range) error {
	return nil
}

type FakeRoster struct {
}

func Get(t *testing.T, ks *ranje.Keyspace, rID uint64) *ranje.Range {
	r, err := ks.Get(ranje.Ident(rID))
	require.NoError(t, err)
	return r
}

func getConfig() config.Config {
	return config.Config{
		DrainNodesBeforeShutdown: false,
		NodeExpireDuration:       0,
		Replication:              2,
	}
}

func TestInitial(t *testing.T) {
	cfg := getConfig()

	disc, err := mockdisc.New()
	require.NoError(t, err)

	ks := ranje.New(cfg, &FakePersister{})
	rost := roster.New(cfg, disc, nil, nil, nil)
	srv := grpc.NewServer()

	// TODO: Remove
	assert.Equal(t, 1, ks.Len())
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", ks.LogString())
	// End

	// TODO: Move to keyspace tests.
	r := Get(t, ks, 1)
	assert.NotNil(t, r)
	assert.Equal(t, ranje.ZeroKey, r.Meta.Start, "range should start at ZeroKey")
	assert.Equal(t, ranje.ZeroKey, r.Meta.End, "range should end at ZeroKey")
	assert.Equal(t, ranje.RsActive, r.State, "range should be born active")
	assert.Equal(t, 0, len(r.Placements), "range should be born with no placements")

	bal := New(cfg, ks, rost, srv)
	assert.Equal(t, "{1 [-inf, +inf] RsActive}", ks.LogString())

	bal.Tick()
	assert.Equal(t, "{1 [-inf, +inf] RsActive p0=TODO:SpUnknown}", ks.LogString())

}
