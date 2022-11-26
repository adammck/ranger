package mirror

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adammck/ranger/pkg/api"
	mock_disc "github.com/adammck/ranger/pkg/discovery/mock"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestEmpty(t *testing.T) {
	h := setup(t)
	res := h.mirror.Find(api.Key("aaa"))
	assert.Assert(t, cmp.Len(res, 0))
}

func TestStatic(t *testing.T) {
	h := setup(t)
	h.add(t, api.Remote{
		Ident: "aaa",
		Host:  "host-aaa",
		Port:  1,
	}, []api.RangeInfo{
		{
			Meta:  api.Meta{Ident: 1, End: api.Key("ggg")},
			State: api.NsActive,
		},
		{
			Meta:  api.Meta{Ident: 2, Start: api.Key("ggg"), End: api.Key("sss")},
			State: api.NsActive,
		},
	})
	h.add(t, api.Remote{
		Ident: "bbb",
		Host:  "host-bbb",
		Port:  2,
	}, []api.RangeInfo{
		{
			Meta:  api.Meta{Ident: 3, Start: api.Key("sss")},
			State: api.NsActive,
		},
	})

	// TODO: Add some kind of sync method to block until the mirror has fetched
	//       something (or nothing) from each node.
	time.Sleep(100 * time.Millisecond)

	res := h.mirror.Find(api.Key("ccc"))
	assert.DeepEqual(t, []Result{{
		RangeID: 1,
		Remote: api.Remote{
			Ident: "aaa",
			Host:  "host-aaa",
			Port:  1,
		},
		State: api.NsActive,
	}}, res)

	res = h.mirror.Find(api.Key("hhh"))
	assert.DeepEqual(t, []Result{{
		RangeID: 2,
		Remote: api.Remote{
			Ident: "aaa",
			Host:  "host-aaa",
			Port:  1,
		},
		State: api.NsActive,
	}}, res)

	res = h.mirror.Find(api.Key("zzz"))
	assert.DeepEqual(t, []Result{{
		RangeID: 3,
		Remote: api.Remote{
			Ident: "bbb",
			Host:  "host-bbb",
			Port:  2,
		},
		State: api.NsActive,
	}}, res)
}

// -----------------------------------------------------------------------------

type testHarness struct {
	disc   *mock_disc.Discoverer
	nodes  map[api.Remote]*nodeServer
	mirror *Mirror
}

func setup(t *testing.T) *testHarness {
	h := &testHarness{
		disc:  mock_disc.NewDiscoverer(),
		nodes: map[api.Remote]*nodeServer{},
	}

	h.mirror = New(h.disc).WithDialler(h.dial) // SUT

	t.Cleanup(func() {
		// Ignore errors
		_ = h.mirror.Stop()
	})

	return h
}

func (h *testHarness) add(t *testing.T, rem api.Remote, ranges []api.RangeInfo) {
	ns := newNodeServer(ranges)
	t.Cleanup(ns.stop)
	h.nodes[rem] = ns
	h.disc.Add("node", rem)
}

func (h *testHarness) dial(ctx context.Context, rem api.Remote) (*grpc.ClientConn, error) {
	node, ok := h.nodes[rem]
	if !ok {
		log.Printf("No such remote: %s", rem.Ident)
		return nil, fmt.Errorf("No such remote: %s", rem.Ident)
	}

	return grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return node.listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

type nodeServer struct {
	pb.UnimplementedNodeServer

	server   *grpc.Server
	listener *bufconn.Listener
	ranges   []api.RangeInfo
	quit     *sync.WaitGroup
}

func newNodeServer(ranges []api.RangeInfo) *nodeServer {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	ns := &nodeServer{
		server:   grpc.NewServer(),
		listener: bufconn.Listen(1024 * 1024),
		ranges:   ranges,
		quit:     wg,
	}

	pb.RegisterNodeServer(ns.server, ns)

	go func() {
		if err := ns.server.Serve(ns.listener); err != nil {
			panic(err)
		}
	}()

	return ns
}

func (ns *nodeServer) stop() {
	ns.server.Stop()
	ns.quit.Done()
}

func (ns *nodeServer) Ranges(req *pb.RangesRequest, stream pb.Node_RangesServer) error {
	for _, ri := range ns.ranges {
		stream.Send(&pb.RangesResponse{
			Meta:  conv.MetaToProto(ri.Meta),
			State: conv.RemoteStateToProto(ri.State),
		})
	}

	ns.quit.Wait()

	return io.EOF
}
