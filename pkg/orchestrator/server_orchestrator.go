package orchestrator

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type orchestratorServer struct {
	pb.UnsafeOrchestratorServer
	orch *Orchestrator
}

func (bs *orchestratorServer) Move(ctx context.Context, req *pb.MoveRequest) (*pb.MoveResponse, error) {
	rID, err := getRange(bs, req.Range, "range")
	if err != nil {
		return nil, err
	}

	op := OpMove{
		Range: rID,
		Dest:  req.Node, // TODO: Verify if given
		Err:   make(chan error),
	}

	bs.orch.opMovesMu.Lock()
	bs.orch.opMoves = append(bs.orch.opMoves, op)
	bs.orch.opMovesMu.Unlock()

	errs := []string{}
	for {
		err, ok := <-op.Err
		if !ok { // closed
			break
		}
		errs = append(errs, err.Error())
	}

	// There's probably only one error. But who knows.
	if len(errs) > 0 {
		return nil, status.Error(
			codes.Aborted,
			fmt.Sprintf("move operation failed: %v", strings.Join(errs, "; ")))
	}

	return &pb.MoveResponse{}, nil
}

func (bs *orchestratorServer) Split(ctx context.Context, req *pb.SplitRequest) (*pb.SplitResponse, error) {
	rID, err := getRange(bs, req.Range, "range")
	if err != nil {
		return nil, err
	}

	boundary := ranje.Key(req.Boundary)
	if boundary == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: boundary")
	}

	op := OpSplit{
		Range: rID,
		Key:   boundary,
		Left:  req.NodeLeft,
		Right: req.NodeRight,
		Err:   make(chan error),
	}

	bs.orch.opSplitsMu.Lock()
	bs.orch.opSplits[rID] = op
	bs.orch.opSplitsMu.Unlock()

	errs := []string{}
	for {
		err, ok := <-op.Err
		if !ok { // closed
			break
		}
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return nil, status.Error(
			codes.Aborted,
			fmt.Sprintf("split operation failed: %v", strings.Join(errs, "; ")))
	}

	return &pb.SplitResponse{}, nil
}

func (bs *orchestratorServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	left, err := getRange(bs, req.RangeLeft, "range_left")
	if err != nil {
		return nil, err
	}

	right, err := getRange(bs, req.RangeRight, "range_right")
	if err != nil {
		return nil, err
	}

	op := OpJoin{
		Left:  left,
		Right: right,
		Dest:  req.Node,
		Err:   make(chan error),
	}

	bs.orch.opJoinsMu.Lock()
	bs.orch.opJoins = append(bs.orch.opJoins, op)
	bs.orch.opJoinsMu.Unlock()

	errs := []string{}
	for {
		err, ok := <-op.Err
		if !ok { // closed
			break
		}
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return nil, status.Error(
			codes.Aborted,
			fmt.Sprintf("join operation failed: %v", strings.Join(errs, "; ")))
	}

	return &pb.JoinResponse{}, nil
}

// getRange examines the given range ident and returns the corresponding Range
// or an error suitable for a gRPC response.
func getRange(bs *orchestratorServer, pbid uint64, field string) (ranje.Ident, error) {
	if pbid == 0 {
		return ranje.ZeroRange, status.Error(codes.InvalidArgument, fmt.Sprintf("missing: %s", field))
	}

	id, err := ranje.IdentFromProto(pbid)
	if err != nil {
		return ranje.ZeroRange, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid %s: %s", field, err.Error()))
	}

	return id, nil
}
