package balancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/adammck/ranger/pkg/operations/join"
	"github.com/adammck/ranger/pkg/operations/move"
	"github.com/adammck/ranger/pkg/operations/split"
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
		return nil, status.Error(codes.InvalidArgument, "missing: node")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(e error) {
		err = e
		wg.Done()
	}

	bs.bal.Operation(&move.MoveOp{
		Keyspace: bs.bal.ks,
		Roster:   bs.bal.rost,
		Done:     cb,
		Range:    *id,
		Node:     nid,
	})

	wg.Wait()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("move operation failed: %v", err))
	}

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

	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(e error) {
		err = e
		wg.Done()
	}

	bs.bal.Operation(&split.SplitOp{
		Keyspace:  bs.bal.ks,
		Roster:    bs.bal.rost,
		Done:      cb,
		Range:     *id,
		Boundary:  boundary,
		NodeLeft:  left,
		NodeRight: right,
	})

	wg.Wait()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("split operation failed: %v", err))
	}

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

	wg := sync.WaitGroup{}
	wg.Add(1)

	cb := func(e error) {
		err = e
		wg.Done()
	}

	bs.bal.Operation(&join.JoinOp{
		Keyspace:   bs.bal.ks,
		Roster:     bs.bal.rost,
		Done:       cb,
		RangeLeft:  *left,
		RangeRight: *right,
		Node:       node,
	})

	wg.Wait()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("join operation failed: %v", err))
	}

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
