package balancer

// TODO: This is not just a balancer any more. It's the God Object. Oh dear.

import (
	"context"
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type debugServer struct {
	pb.UnsafeDebugServer
	bal *Balancer
}

func (srv *debugServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	if req.Range == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("IdentFromProto failed: %v", err))
	}

	r, err := srv.bal.ks.GetByIdent(*rID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("GetByIdent failed: %v", err))
	}

	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	res := &pb.RangeResponse{
		Meta:  r.Meta.ToProto(),
		State: r.State().ToProto(),
	}

	if p := r.Placement(); p != nil {
		res.CurrentPlacement = &pb.PlacementTwo{
			Node:  p.NodeID(),
			State: p.State().ToProto(),
		}
	}

	if p := r.NextPlacement(); p != nil {
		res.NextPlacement = &pb.PlacementTwo{
			Node:  p.NodeID(),
			State: p.State().ToProto(),
		}
	}

	return res, nil
}
