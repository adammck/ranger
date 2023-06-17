package rangelet

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/test/fake_storage"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
	"gotest.tools/assert"
)

type rangeInfos map[api.RangeID]*api.RangeInfo

func TestRanges(t *testing.T) {
	c := clockwork.NewFakeClock()
	h := setup(t, singleRange(c))
	req := &pb.RangesRequest{}

	res, err := h.client.Ranges(h.ctx, req)
	assert.NilError(t, err)

	r, err := res.Recv()
	assert.NilError(t, err)
	assert.DeepEqual(t, &pb.RangesResponse{
		Meta: &pb.RangeMeta{
			Ident: 1,
		},
		State: pb.RangeNodeState_ACTIVE,
	}, r, protocmp.Transform())

	err = h.rglt.ForceDrop(1)
	assert.NilError(t, err)

	r, err = res.Recv()
	assert.NilError(t, err)
	assert.DeepEqual(t, &pb.RangesResponse{
		Meta: &pb.RangeMeta{
			Ident: 1,
		},
		State: pb.RangeNodeState_NOT_FOUND,
	}, r, protocmp.Transform())

	err = res.CloseSend()
	assert.NilError(t, err)
}

type testHarness struct {
	ctx    context.Context
	rglt   *Rangelet
	client pb.NodeClient
}

func setup(t *testing.T, ri rangeInfos) *testHarness {
	ctx := context.Background()

	c := clockwork.NewFakeClock()
	stor := fake_storage.NewFakeStorage(ri)
	rglt := newRangelet(c, nil, stor)
	ns := newNodeServer(rglt) // <-- SUT
	srv := grpc.NewServer()
	ns.Register(srv)

	// client
	conn, closer := nodeServer(ctx, srv)
	t.Cleanup(closer)

	client := pb.NewNodeClient(conn)

	return &testHarness{
		ctx:    ctx,
		rglt:   rglt,
		client: client,
	}
}

func singleRange(c clockwork.Clock) rangeInfos {
	return rangeInfos{
		1: {
			Meta:   api.Meta{Ident: 1},
			State:  api.NsActive,
			Expire: c.Now().Add(10 * time.Second),
		},
	}
}

// From: https://harrigan.xyz/blog/testing-go-grpc-server-using-an-in-memory-buffer-with-bufconn/
func nodeServer(ctx context.Context, s *grpc.Server) (*grpc.ClientConn, func()) {
	listener := bufconn.Listen(1024 * 1024)

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())

	return conn, s.Stop
}
