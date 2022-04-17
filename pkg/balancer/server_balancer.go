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
	if r == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rID, err := ranje.IdentFromProto(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	nID := req.Node
	if nID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: node")
	}

	// wg := sync.WaitGroup{}
	// wg.Add(1)

	// cb := func(e error) {
	// 	err = e
	// 	wg.Done()
	// }

	bs.bal.opMovesMu.Lock()
	defer bs.bal.opMovesMu.Unlock()

	// TODO: Probably add a method to do this.
	bs.bal.opMoves = append(bs.bal.opMoves, OpMove{
		Range: rID,
		Dest:  nID,
	})

	// bs.bal.Operation(&move.MoveOp{
	// 	Keyspace: bs.bal.ks,
	// 	Roster:   bs.bal.rost,
	// 	Done:     cb,
	// 	Range:    id,
	// 	Node:     nid,
	// })

	//wg.Wait()

	if err != nil {
		return nil, status.Error(codes.Aborted, fmt.Sprintf("move operation failed: %v", err))
	}

	return &pb.MoveResponse{}, nil
}

func (bs *balancerServer) Split(ctx context.Context, req *pb.SplitRequest) (*pb.SplitResponse, error) {
	// TODO: Restore this now that splits are back.
	panic("not implemented; see bbad4b6")
}

func (bs *balancerServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	panic("not implemented; see bbad4b6")
}
