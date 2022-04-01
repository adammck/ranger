package balancer

import (
	"context"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type balancerServer struct {
	pb.UnsafeBalancerServer
	bal *Balancer
}

func (bs *balancerServer) Move(ctx context.Context, req *pb.MoveRequest) (*pb.MoveResponse, error) {
	panic("not implemented; see bbad4b6")
}

func (bs *balancerServer) Split(ctx context.Context, req *pb.SplitRequest) (*pb.SplitResponse, error) {
	panic("not implemented; see bbad4b6")
}

func (bs *balancerServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	panic("not implemented; see bbad4b6")
}
