package orchestrator

import (
	"context"
	"fmt"

	"github.com/adammck/ranger/pkg/keyspace"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type debugServer struct {
	pb.UnsafeDebugServer
	orch *Orchestrator
}

func rangeResponse(r *ranje.Range, rost *roster.Roster) *pb.RangeResponse {
	parents := make([]uint64, len(r.Parents))
	for i, rID := range r.Parents {
		parents[i] = conv.RangeIDToProto(rID)
	}

	children := make([]uint64, len(r.Children))
	for i, rID := range r.Children {
		children[i] = conv.RangeIDToProto(rID)
	}

	res := &pb.RangeResponse{
		Meta:     conv.MetaToProto(r.Meta),
		State:    conv.RangeStateToProto(r.State),
		Parents:  parents,
		Children: children,
	}

	for _, p := range r.Placements {
		plc := &pb.PlacementWithRangeInfo{
			Placement: &pb.Placement{
				Node:  conv.NodeIDToProto(p.NodeID),
				State: conv.PlacementStateToProto(p.StateCurrent),
			},
		}

		// If RangeInfo is available include it.
		// Might not be, if the node has just vanished or forgotten the range.
		nod, _ := rost.NodeByIdent(p.NodeID)
		if nod != nil {
			if ri, ok := nod.Get(r.Meta.Ident); ok {
				plc.RangeInfo = conv.RangeInfoToProto(ri)
			}
		}

		res.Placements = append(res.Placements, plc)
	}

	return res
}

func nodeResponse(ks *keyspace.Keyspace, n *roster.Node) *pb.NodeResponse {
	res := &pb.NodeResponse{
		Node: &pb.NodeMeta{
			Ident:     conv.NodeIDToProto(n.Ident()),
			Address:   n.Addr(),
			WantDrain: n.WantDrain(),
		},
	}

	for _, pl := range ks.PlacementsByNodeID(n.Ident()) {
		res.Ranges = append(res.Ranges, &pb.NodeRange{
			Meta:  conv.MetaToProto(pl.Range.Meta),
			State: conv.PlacementStateToProto(pl.Placement.StateCurrent),
		})
	}

	return res
}

func (srv *debugServer) RangesList(ctx context.Context, req *pb.RangesListRequest) (*pb.RangesListResponse, error) {
	res := &pb.RangesListResponse{}

	ranges, unlocker := srv.orch.ks.Ranges()
	defer unlocker()

	for _, r := range ranges {
		r.Mutex.Lock()
		res.Ranges = append(res.Ranges, rangeResponse(r, srv.orch.rost))
		r.Mutex.Unlock()
	}

	return res, nil
}

func (srv *debugServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	if req.Range == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rID, err := conv.RangeIDFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("IdentFromProto failed: %v", err))
	}

	r, err := srv.orch.ks.GetRange(rID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("GetByIdent failed: %v", err))
	}

	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	res := rangeResponse(r, srv.orch.rost)

	return res, nil
}

func (srv *debugServer) Node(ctx context.Context, req *pb.NodeRequest) (*pb.NodeResponse, error) {
	nID, err := conv.NodeIDFromProto(req.Node)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("NodeIDFromProto failed: %v", err))
	}

	node, err := srv.orch.rost.NodeByIdent(nID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	res := nodeResponse(srv.orch.ks, node)

	return res, nil
}

func (srv *debugServer) NodesList(ctx context.Context, req *pb.NodesListRequest) (*pb.NodesListResponse, error) {
	rost := srv.orch.rost
	rost.RLock()
	defer rost.RUnlock()

	res := &pb.NodesListResponse{}

	for _, n := range rost.Nodes {
		res.Nodes = append(res.Nodes, nodeResponse(srv.orch.ks, n))
	}

	return res, nil
}
