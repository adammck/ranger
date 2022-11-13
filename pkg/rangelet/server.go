package rangelet

import (
	"context"

	"github.com/adammck/ranger/pkg/api"
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
	return ns
}

func (ns *NodeServer) Register(sr grpc.ServiceRegistrar) {
	pb.RegisterNodeServer(sr, ns)
}

func parentsFromProto(prot []*pb.Parent) ([]api.Parent, error) {
	p := []api.Parent{}

	for _, pp := range prot {
		m, err := ranje.MetaFromProto(pp.Range)
		if err != nil {
			return p, err
		}

		parentIds := make([]api.Ident, len(pp.Parent))
		for i := range pp.Parent {
			parentIds[i] = api.Ident(pp.Parent[i])
		}

		placements := make([]api.Placement, len(pp.Placements))
		for i := range pp.Placements {
			placements[i] = api.Placement{
				Node:  pp.Placements[i].Node,
				State: ranje.PlacementStateFromProto(&pp.Placements[i].State),
			}
		}

		p = append(p, api.Parent{
			Meta:       *m,
			Parents:    parentIds,
			Placements: placements,
		})
	}

	return p, nil
}

func (ns *NodeServer) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	parents, err := parentsFromProto(req.Parents)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing parents: %v", err)
	}

	ri, err := ns.r.give(*meta, parents)
	if err != nil {
		return nil, err
	}

	return &pb.GiveResponse{
		RangeInfo: info.RangeInfoToProto(ri),
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
		State: state.RemoteStateToProto(ri.State),
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
		State: state.RemoteStateToProto(ri.State),
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
				State: state.RemoteStateToProto(api.NsNotFound),
			}, nil
		}

		// But other errors are.
		return nil, err
	}

	return &pb.DropResponse{
		State: state.RemoteStateToProto(ri.State),
	}, nil
}

func (ns *NodeServer) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	err := ns.r.gatherLoadInfo()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	res := &pb.InfoResponse{
		WantDrain: ns.r.wantDrain(),
	}

	ns.r.walk(func(ri *api.RangeInfo) {
		res.Ranges = append(res.Ranges, info.RangeInfoToProto(*ri))
	})

	return res, nil
}

// TODO: Has this been subsumed by Info? Nobody seems to call it.
func (ns *NodeServer) Ranges(ctx context.Context, req *pb.RangesRequest) (*pb.RangesResponse, error) {
	panic("not imlemented!")
}
