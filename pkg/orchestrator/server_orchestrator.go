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

	nID := req.Node
	if nID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node")
	}

	errCh := make(chan error)

	func() {
		bs.orch.opMovesMu.Lock()
		defer bs.orch.opMovesMu.Unlock()

		// TODO: Probably add a method to do this.
		bs.orch.opMoves = append(bs.orch.opMoves, OpMove{
			Range: rID,
			Dest:  nID,
			Err:   errCh,
		})
	}()

	errs := []string{}

	for {
		err, ok := <-errCh
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

	// TODO: Allow destination node(s) to be specified.
	// TODO: Block until split is complete, like move does.

	func() {
		bs.orch.opSplitsMu.Lock()
		defer bs.orch.opSplitsMu.Unlock()
		bs.orch.opSplits[rID] = OpSplit{
			Range: rID,
			Key:   boundary,
		}
	}()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("split operation failed: %v", err))
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

	// TODO: Allow destination node(s) to be specified.
	// TODO: Block until join is complete, like move does.

	func() {
		bs.orch.opJoinsMu.Lock()
		defer bs.orch.opJoinsMu.Unlock()

		// TODO: Probably add a method to do this.
		bs.orch.opJoins = append(bs.orch.opJoins, OpJoin{
			Left:  left,
			Right: right,
		})
	}()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("join operation failed: %v", err))
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
