package rpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/adammck/ranger/pkg/api"
	mockdisc "github.com/adammck/ranger/pkg/discovery/mock"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestPrepare(t *testing.T) {
	h := setup(t)
	p := getPlacement(t, h.rangeGetter, 2, 0)
	n, err := h.nodeGetter.NodeByIdent("node-aaa")
	assert.NilError(t, err)

	cmd := api.Command{
		RangeIdent: 2,
		NodeIdent:  "node-aaa",
		Action:     api.Prepare,
	}

	// success

	err = h.actuator.Command(cmd, p, n)
	assert.NilError(t, err)

	assert.Assert(t, h.node.prepareReq != nil)
	assert.DeepEqual(t, pb.PrepareRequest{
		Range: &pb.RangeMeta{
			Ident: 2,
			End:   []byte("ccc"),
		},
		Parents: []*pb.Parent{
			{
				Range: &pb.RangeMeta{
					Ident: 2,
					End:   []byte("ccc"),
				},
				Parent: []uint64{},
				Placements: []*pb.Placement{
					{
						Node:  "host-aaa:8001", // TODO: wtf
						State: pb.PlacementState_PS_ACTIVE,
					},
				},
			},
			{
				Range: &pb.RangeMeta{
					Ident: 1,
				},
				Parent: []uint64{},
				Placements: []*pb.Placement{
					{
						Node:  "host-aaa:8001",
						State: pb.PlacementState_PS_INACTIVE,
					},
				},
			},
		},
	}, h.node.prepareReq, protocmp.Transform())

	// error

	h.node.prepareErr = status.Errorf(codes.InvalidArgument, "injected")

	err = h.actuator.Command(cmd, p, n)
	assert.Error(t, err, "rpc error: code = InvalidArgument desc = injected")
}

func TestServe(t *testing.T) {
	h := setup(t)
	p := getPlacement(t, h.rangeGetter, 3, 0)
	n, err := h.nodeGetter.NodeByIdent("node-aaa")
	assert.NilError(t, err)

	cmd := api.Command{
		RangeIdent: 3,
		NodeIdent:  "node-aaa",
		Action:     api.Serve,
	}

	// success

	err = h.actuator.Command(cmd, p, n)
	assert.NilError(t, err)

	assert.Assert(t, h.node.serveReq != nil)
	assert.DeepEqual(t, pb.ServeRequest{
		Range: 3,
		Force: false,
	}, h.node.serveReq, protocmp.Transform())

	// error

	h.node.serveErr = status.Errorf(codes.FailedPrecondition, "injected")

	err = h.actuator.Command(cmd, p, n)
	assert.Error(t, err, "rpc error: code = FailedPrecondition desc = injected")
}

func TestTake(t *testing.T) {
	h := setup(t)
	p := getPlacement(t, h.rangeGetter, 3, 0)
	n, err := h.nodeGetter.NodeByIdent("node-aaa")
	assert.NilError(t, err)

	cmd := api.Command{
		RangeIdent: 3,
		NodeIdent:  "node-aaa",
		Action:     api.Take,
	}

	// success

	err = h.actuator.Command(cmd, p, n)
	assert.NilError(t, err)

	assert.Assert(t, h.node.takeReq != nil)
	assert.DeepEqual(t, pb.TakeRequest{
		Range: 3,
	}, h.node.takeReq, protocmp.Transform())

	// error

	h.node.takeErr = status.Errorf(codes.Aborted, "injected")

	err = h.actuator.Command(cmd, p, n)
	assert.Error(t, err, "rpc error: code = Aborted desc = injected")
}

func TestDrop(t *testing.T) {
	h := setup(t)
	p := getPlacement(t, h.rangeGetter, 3, 0)
	n, err := h.nodeGetter.NodeByIdent("node-aaa")
	assert.NilError(t, err)

	cmd := api.Command{
		RangeIdent: 3,
		NodeIdent:  "node-aaa",
		Action:     api.Drop,
	}

	// success

	err = h.actuator.Command(cmd, p, n)
	assert.NilError(t, err)

	assert.Assert(t, h.node.dropReq != nil)
	assert.DeepEqual(t, pb.DropRequest{
		Range: 3,
		Force: false,
	}, h.node.dropReq, protocmp.Transform())

	// error

	h.node.dropErr = status.Errorf(codes.NotFound, "injected")

	err = h.actuator.Command(cmd, p, n)
	assert.Error(t, err, "rpc error: code = NotFound desc = injected")
}

func TestInvalidAction(t *testing.T) {
	h := setup(t)

	r, err := h.rangeGetter.GetRange(1)

	assert.NilError(t, err)
	p := r.Placements[0]
	require.NotNil(t, p)

	n, err := h.nodeGetter.NodeByIdent("node-aaa")
	assert.NilError(t, err)

	invalid := api.Action(99)

	cmd := api.Command{
		RangeIdent: r.Meta.Ident,
		NodeIdent:  n.Ident(),
		Action:     invalid,
	}

	assert.Assert(t, cmp.Panics(func() {
		h.actuator.Command(cmd, p, n)
	}))
}

// -------------------------------------------------------------- RangeGetter --

type FakeRangeGetter struct {
	r map[api.RangeID]*ranje.Range
}

func NewFakeRangeGetter(ranges ...*ranje.Range) *FakeRangeGetter {
	m := map[api.RangeID]*ranje.Range{}
	for _, r := range ranges {
		m[r.Meta.Ident] = r
	}

	return &FakeRangeGetter{
		r: m,
	}
}

func (rg *FakeRangeGetter) GetRange(id api.RangeID) (*ranje.Range, error) {
	r, ok := rg.r[id]
	if !ok {
		return nil, fmt.Errorf("no such range: %s", id)
	}
	return r, nil
}

// --------------------------------------------------------------- NodeGetter --

type FakeNodeGetter struct {
	n map[api.NodeID]*roster.Node
}

func (rg *FakeNodeGetter) NodeByIdent(nID api.NodeID) (*roster.Node, error) {
	n, ok := rg.n[nID]
	if !ok {
		return nil, roster.ErrNodeNotFound{NodeID: nID}
	}
	return n, nil
}

// ----------------------------------------------------------------- TestNode --

type NodeServer struct {
	pb.UnsafeNodeServer

	prepareErr error
	prepareReq *pb.PrepareRequest

	serveReq *pb.ServeRequest
	serveErr error

	takeReq *pb.TakeRequest
	takeErr error

	dropReq *pb.DropRequest
	dropErr error
}

func (ns *NodeServer) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	ns.prepareReq = req

	if ns.prepareErr != nil {
		return nil, ns.prepareErr
	}

	return &pb.PrepareResponse{
		RangeInfo: &pb.RangeInfo{
			Meta: &pb.RangeMeta{
				Ident: 0,
				Start: []byte{},
				End:   []byte{},
			},
			State: 0,
			Info: &pb.LoadInfo{
				Keys:   0,
				Splits: []string{},
			},
		},
	}, nil
}

func (ns *NodeServer) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
	ns.serveReq = req

	if ns.serveErr != nil {
		return nil, ns.serveErr
	}

	return &pb.ServeResponse{
		State: pb.RangeNodeState_ACTIVE,
	}, nil
}

func (ns *NodeServer) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
	ns.takeReq = req

	if ns.takeErr != nil {
		return nil, ns.takeErr
	}

	return &pb.TakeResponse{
		State: pb.RangeNodeState_INACTIVE,
	}, nil
}

func (ns *NodeServer) Drop(ctx context.Context, req *pb.DropRequest) (*pb.DropResponse, error) {
	ns.dropReq = req

	if ns.dropErr != nil {
		return nil, ns.dropErr
	}

	return &pb.DropResponse{
		State: pb.RangeNodeState_NOT_FOUND,
	}, nil
}

func (ns *NodeServer) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ns *NodeServer) Ranges(req *pb.RangesRequest, stream pb.Node_RangesServer) error {
	return fmt.Errorf("not implemented")
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

// ------------------------------------------------------------------ Harness --

type Harness struct {
	discovery   *mockdisc.MockDiscovery
	node        *NodeServer
	rangeGetter *FakeRangeGetter
	nodeGetter  *FakeNodeGetter
	actuator    *Actuator
}

func setupDiscovery() (*mockdisc.MockDiscovery, api.Remote) {
	d := mockdisc.New()

	da := api.Remote{
		Ident: "node-aaa",
		Host:  "host-aaa",
		Port:  8001,
	}

	d.Add("node", da)

	return d, da
}

func setupRangeGetter() *FakeRangeGetter {

	//      ┌─────┐
	//    ┌─│ 1 s │─┐
	//    │ └─────┘ │
	//    ▼         ▼
	// ┌─────┐   ┌─────┐
	// │ 2 a │   │ 3 a │
	// └─────┘   └─────┘
	//
	// R1 splitting into R2, R3 at ccc
	// R2 active (splitting from R1)
	// R3 active (splitting from R1)

	r1 := &ranje.Range{
		State:    api.RsSubsuming,
		Children: []api.RangeID{2, 3},
		Meta:     api.Meta{Ident: 1, Start: api.ZeroKey, End: api.ZeroKey},
	}
	r1p0 := ranje.NewPlacement(r1, "node-aaa")
	r1p0.StateCurrent = api.PsInactive
	r1p0.StateDesired = api.PsInactive
	r1.Placements = []*ranje.Placement{r1p0}

	r2 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 2, End: api.Key("ccc")},
	}
	r2p0 := ranje.NewPlacement(r2, "node-aaa")
	r2p0.StateCurrent = api.PsActive
	r2p0.StateDesired = api.PsActive
	r2.Placements = []*ranje.Placement{r2p0}

	r3 := &ranje.Range{
		State:    api.RsActive,
		Parents:  []api.RangeID{1},
		Children: []api.RangeID{},
		Meta:     api.Meta{Ident: 3, Start: api.Key("ccc")},
	}
	r3p0 := ranje.NewPlacement(r3, "node-aaa")
	r3p0.StateCurrent = api.PsActive
	r3p0.StateDesired = api.PsActive
	r3.Placements = []*ranje.Placement{r3p0}

	return NewFakeRangeGetter(r1, r2, r3)
}

func setup(t *testing.T) *Harness {
	ctx := context.Background()

	srv := grpc.NewServer()
	n1 := &NodeServer{}
	pb.RegisterNodeServer(srv, n1)

	conn, closer := nodeServer(ctx, srv)
	t.Cleanup(closer)

	d, da := setupDiscovery()

	ng := &FakeNodeGetter{
		n: map[api.NodeID]*roster.Node{
			da.NodeID(): roster.NewNode(da, conn),
		},
	}

	rg := setupRangeGetter()
	a := New(rg, ng)

	return &Harness{
		discovery:   d,
		node:        n1,
		rangeGetter: rg,
		nodeGetter:  ng,
		actuator:    a,
	}
}

func getPlacement(t *testing.T, rg *FakeRangeGetter, rID api.RangeID, i int) *ranje.Placement {
	r2, err := rg.GetRange(rID)
	assert.NilError(t, err)
	assert.Assert(t, len(r2.Placements) >= i+1)
	p := r2.Placements[i]
	require.NotNil(t, p)
	return p
}
