package balancer

import (
	"context"
	"fmt"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type balancerServer struct {
	pb.UnsafeBalancerServer
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

	err = bs.bal.operatorForce(id, n)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.ForceResponse{}, nil
}

func (bs *balancerServer) Split(ctx context.Context, req *pb.SplitRequest) (*pb.SplitResponse, error) {
	pbid := req.Range
	if pbid == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	id, err := ranje.IdentFromProto(pbid)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, err := bs.bal.getRange(*id)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// TODO: Is this a good idea?
	// We could just record the split point, return OK, and try our best.
	err = bs.bal.rangeCanBeSplit(r)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	boundary := ranje.Key(req.Boundary)
	if boundary == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: boundary")
	}

	left := req.NodeLeft
	if left == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node_left")
	}

	right := req.NodeRight
	if right == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node_right")
	}

	err = bs.bal.operatorSplit(r, boundary, left, right)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.SplitResponse{}, nil
}

// getRange examines the given range ident and returns the corresponding Range
// or an error suitable for a gRPC response.
func getRange(bs *balancerServer, pbid *pb.Ident, field string) (*ranje.Range, error) {
	if pbid == nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("missing: %s", field))
	}

	id, err := ranje.IdentFromProto(pbid)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid %s: %s", field, err.Error()))
	}

	r, err := bs.bal.getRange(*id)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return r, nil
}

func (bs *balancerServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	left, err := getRange(bs, req.RangeLeft, "range_left")
	if err != nil {
		return nil, err
	}

	right, err := getRange(bs, req.RangeRight, "range_right")
	if err != nil {
		return nil, err
	}

	node := req.Node
	if node == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node")
	}

	err = bs.bal.operatorJoin(left, right, node)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.JoinResponse{}, nil
}
