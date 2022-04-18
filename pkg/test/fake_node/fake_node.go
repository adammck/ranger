package fake_node

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
	"github.com/adammck/ranger/pkg/roster/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testRange struct {
	Info *info.RangeInfo
}

// TODO: Most of this should be moved into a client library. Rangelet?
type TestNode struct {
	pb.UnimplementedNodeServer

	// TODO: Move this to an outer object, with srv and ranges. We're currently
	//       using the nodeServer for both, which is pretty weird.
	TestRanges map[ranje.Ident]*testRange
	sync.RWMutex

	rpcs []interface{}
}

func NewTestNode(rangeInfos map[ranje.Ident]*info.RangeInfo) *TestNode {
	ranges := map[ranje.Ident]*testRange{}

	for rID, ri := range rangeInfos {
		ranges[rID] = &testRange{Info: ri}
	}

	return &TestNode{
		TestRanges: ranges,
	}
}

func (n *TestNode) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	log.Printf("TestNode.Give")
	n.rpcs = append(n.rpcs, req)

	meta, err := ranje.MetaFromProto(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range meta: %v", err)
	}

	var ri *info.RangeInfo

	r, ok := n.getRange(meta.Ident)
	if !ok {
		ri = &info.RangeInfo{
			Meta:  *meta,
			State: state.NsPreparing,
		}
		func() {
			n.Lock()
			defer n.Unlock()
			n.TestRanges[ri.Meta.Ident] = &testRange{
				Info: ri,
			}
		}()
	} else {
		switch r.Info.State {
		case state.NsPreparing, state.NsPreparingError, state.NsPrepared:
			// We already know about this range, and it's in one of the states
			// that indicate a previous Give. This is a duplicate. Don't change
			// any state, just return the RangeInfo to let the controller know
			// how we're doing.
			ri = r.Info
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid state for redundant Give: %v", r.Info.State)
		}
	}

	return &pb.GiveResponse{
		RangeInfo: ri.ToProto(),
	}, nil
}

func (n *TestNode) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
	log.Printf("TestNode.Serve")
	n.rpcs = append(n.rpcs, req)

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.getRange(rID)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Serve unknown range: %v", rID)
	}

	switch r.Info.State {
	case state.NsPrepared:
		// Actual state transition.
		r.Info.State = state.NsReadying

	case state.NsReadying, state.NsReady:
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
	n.rpcs = append(n.rpcs, req)

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.getRange(rID)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "can't Take unknown range: %v", rID)
	}

	switch r.Info.State {
	case state.NsReady:
		// Actual state transition.
		r.Info.State = state.NsTaking

	case state.NsTaking, state.NsTaken:
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
	n.rpcs = append(n.rpcs, req)

	rID, err := ranje.IdentFromProto(req.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	r, ok := n.getRange(rID)
	if !ok {
		log.Printf("got redundant Drop (no such range; maybe drop complete)")

		// This is NOT a failure.
		return &pb.DropResponse{
			State: state.NsNotFound.ToProto(),
		}, nil
	}

	switch r.Info.State {
	case state.NsTaken:
		// Actual state transition. We don't actually drop anything here, only
		// claim that we are doing so, to simulate a slow client. Test must call
		// FinishDrop to move to NsDropped.
		r.Info.State = state.NsDropping

	case state.NsDropping:
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

func (n *TestNode) getRange(rID ranje.Ident) (*testRange, bool) {
	n.RLock()
	defer n.RUnlock()
	tr, ok := n.TestRanges[rID]
	return tr, ok
}

// RPCs returns a slice of the (proto) requests received by this node since the
// last time that this method was called, in a deterministic-ish order. This is
// an ugly hack because asserting that an unordered bunch of protos were all
// sent is hard.
func (n *TestNode) RPCs() []interface{} {
	ret := n.rpcs
	n.rpcs = nil

	val := func(i int) int {

		switch v := ret[i].(type) {
		case *pb.GiveRequest:
			return 100 + int(v.Range.Ident)

		case *pb.ServeRequest:
			return 200 + int(v.Range)

		case *pb.TakeRequest:
			return 300 + int(v.Range)

		case *pb.DropRequest:
			return 400 + int(v.Range)

		default:
			panic(fmt.Sprintf("unhandled type: %T\n", v))
		}
	}

	// Sort by the Ident of the Range.
	sort.Slice(ret, func(i, j int) bool {
		return val(i) < val(j)
	})

	return ret
}
