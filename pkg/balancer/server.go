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

func (bs *balancerServer) Move(ctx context.Context, req *pb.MoveRequest) (*pb.MoveResponse, error) {
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	id, err := ranje.IdentFromProto(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	nid := req.Node
	if nid == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	bs.bal.Operation(MoveRequest{
		Range: *id,
		Node:  nid,
	})

	return &pb.MoveResponse{}, nil
}

func (bs *balancerServer) Split(ctx context.Context, req *pb.SplitRequest) (*pb.SplitResponse, error) {
	id, err := getRange(bs, req.Range, "range")
	if err != nil {
		return nil, err
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

	bs.bal.Operation(SplitRequest{
		Range:     *id,
		Boundary:  boundary,
		NodeLeft:  left,
		NodeRight: right,
	})

	return &pb.SplitResponse{}, nil
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

	bs.bal.Operation(JoinRequest{
		Left:  *left,
		Right: *right,
		Node:  node,
	})

	return &pb.JoinResponse{}, nil
}

// getRange examines the given range ident and returns the corresponding Range
// or an error suitable for a gRPC response.
func getRange(bs *balancerServer, pbid *pb.Ident, field string) (*ranje.Ident, error) {
	if pbid == nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("missing: %s", field))
	}

	id, err := ranje.IdentFromProto(pbid)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid %s: %s", field, err.Error()))
	}

	return id, nil
}
