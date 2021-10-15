package balancer

import (
	"context"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type balancerServer struct {
	pb.UnimplementedBalancerServer
	bal *Balancer
}

func (bs *balancerServer) Force(ctx context.Context, req *pb.ForceRequest) (*pb.ForceResponse, error) {
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	id, err := ranje.IdentFromProto(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	n := req.Node
	if n == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	err = bs.bal.Force(id, n)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.ForceResponse{}, nil
}
