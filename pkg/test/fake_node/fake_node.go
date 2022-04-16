package fake_node

import (
	"context"
	"log"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testRange struct {
	Info *roster.RangeInfo
}

// TODO: Most of this should be moved into a client library. Rangelet?
type TestNode struct {
	pb.UnimplementedNodeServer

	// TODO: Move this to an outer object, with srv and ranges. We're currently
	//       using the nodeServer for both, which is pretty weird.
	TestRanges map[ranje.Ident]*testRange
}

func NewTestNode(rangeInfos map[ranje.Ident]*roster.RangeInfo) *TestNode {
	ranges := map[ranje.Ident]*testRange{}
	//if rangeInfos != nil
	for rID, ri := range rangeInfos {
		ranges[rID] = &testRange{Info: ri}
	}

	return &TestNode{
		TestRanges: ranges,
	}
}

func (n *TestNode) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	log.Printf("TestNode.Give")

	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	var info *roster.RangeInfo

	r, ok := n.TestRanges[meta.Ident]
	if !ok {
		info = &roster.RangeInfo{
			Meta:  *meta,
			State: roster.NsPreparing,
		}
		n.TestRanges[info.Meta.Ident] = &testRange{
			Info: info,
		}
	} else {
		switch r.Info.State {
		case roster.NsPreparing, roster.NsPreparingError, roster.NsPrepared:
			// We already know about this range, and it's in one of the states
			// that indicate a previous Give. This is a duplicate. Don't change
			// any state, just return the RangeInfo to let the controller know
			// how we're doing.
			info = r.Info
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid state for redundant Give: %v", r.Info.State)
		}
	}

	return &pb.GiveResponse{
		RangeInfo: info.ToProto(),
	}, nil
}

func (n *TestNode) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
	log.Printf("TestNode.Serve")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.TestRanges[rID]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	switch r.Info.State {
	case roster.NsPrepared:
		// Actual state transition.
		r.Info.State = roster.NsReadying

	case roster.NsReadying, roster.NsReady:
		log.Printf("got redundant Serve")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Serve: %v", r.Info.State)
	}

	return &pb.ServeResponse{
		State: r.Info.State.ToProto(),
	}, nil
}

func (n *TestNode) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
	log.Printf("TestNode.Take")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.TestRanges[rID]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	switch r.Info.State {
	case roster.NsReady:
		// Actual state transition.
		r.Info.State = roster.NsTaking

	case roster.NsTaking, roster.NsTaken:
		log.Printf("got redundant Take")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Take: %v", r.Info.State)
	}

	return &pb.TakeResponse{
		State: r.Info.State.ToProto(),
	}, nil
}

func (n *TestNode) Drop(ctx context.Context, req *pb.DropRequest) (*pb.DropResponse, error) {
	log.Printf("TestNode.Drop")

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.TestRanges[rID]
	if !ok {
		log.Printf("got redundant Drop (no such range; maybe drop complete)")

		// This is NOT a failure.
		return &pb.DropResponse{
			State: roster.NsNotFound.ToProto(),
		}, nil
	}

	switch r.Info.State {
	case roster.NsTaken:
		// Actual state transition. We don't actually drop anything here, only
		// claim that we are doing so, to simulate a slow client. Test must call
		// FinishDrop to move to NsDropped.
		r.Info.State = roster.NsDropping

	case roster.NsDropping:
		log.Printf("got redundant Drop (drop in progress)")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid state for Drop: %v", r.Info.State)
	}

	return &pb.DropResponse{
		State: r.Info.State.ToProto(),
	}, nil
}

func (n *TestNode) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	log.Printf("TestNode.Info")

	res := &pb.InfoResponse{
		WantDrain: false,
	}

	for _, r := range n.TestRanges {
		res.Ranges = append(res.Ranges, r.Info.ToProto())
	}

	log.Printf("res: %v", res)
	return res, nil
}

func (n *TestNode) Ranges(ctx context.Context, req *pb.RangesRequest) (*pb.RangesResponse, error) {
	panic("not imlemented!")
}
