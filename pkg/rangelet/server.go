package rangelet

import (
	"context"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NodeServer struct {
	pb.UnsafeNodeServer
	r *Rangelet
}

func NewNodeServer(rangelet *Rangelet) *NodeServer {
	ns := &NodeServer{r: rangelet}

	s := grpc.NewServer()
	pb.RegisterNodeServer(s, ns)

	return ns
}

func (ns *NodeServer) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	ri, err := ns.r.give(*meta)
	if err != nil {
		return nil, err
	}

	return &pb.GiveResponse{
		RangeInfo: ri.ToProto(),
	}, nil
}

func (ns *NodeServer) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ri, err := ns.r.serve(rID)
	if err != nil {
		return nil, err
	}

	return &pb.ServeResponse{
		State: ri.State.ToProto(),
	}, nil
}

func (ns *NodeServer) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ri, err := ns.r.take(rID)
	if err != nil {
		return nil, err
	}

	return &pb.TakeResponse{
		State: ri.State.ToProto(),
	}, nil
}

func (ns *NodeServer) Drop(ctx context.Context, req *pb.DropRequest) (*pb.DropResponse, error) {

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ri, err := ns.r.drop(rID)
	if err != nil {
		// This is NOT a failure.
		if err == ErrNotFound {
			return &pb.DropResponse{
				State: state.NsNotFound.ToProto(),
			}, nil
		}

		// But other errors are.
		return nil, err
	}

	return &pb.DropResponse{
		State: ri.State.ToProto(),
	}, nil
}

func (ns *NodeServer) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	res := &pb.InfoResponse{
		WantDrain: ns.r.wantDrain(),
	}

	ns.r.walk(func(ri *info.RangeInfo) {
		res.Ranges = append(res.Ranges, ri.ToProto())
	})

	return res, nil
}

// TODO: Has this been subsumed by Info? Nobody seems to call it.
func (ns *NodeServer) Ranges(ctx context.Context, req *pb.RangesRequest) (*pb.RangesResponse, error) {
	panic("not imlemented!")
}

// TODO: Remove this method? Taken->Ready is no longer valid.
func (ns *NodeServer) Untake(ctx context.Context, req *pb.UntakeRequest) (*pb.UntakeResponse, error) {
	panic("not imlemented!")
}
