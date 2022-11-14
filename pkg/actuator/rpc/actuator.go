package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/adammck/ranger/pkg/actuator/util"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

type Actuator struct {
	rg util.RangeGetter
	ng util.NodeGetter
}

const rpcTimeout = 1 * time.Second

func New(rg util.RangeGetter, ng util.NodeGetter) *Actuator {
	return &Actuator{
		rg: rg,
		ng: ng,
	}
}

// TODO: This is currently duplicated.
// TODO: This interface should probably only take the command -- the placement
//       and node can be fetched from the Getters if needed.
func (a *Actuator) Command(cmd api.Command, p *ranje.Placement, n *roster.Node) error {
	s, err := a.cmd(cmd.Action, p, n)
	if err != nil {
		return err
	}

	// TODO: This special case is weird. It was less so when Give was a
	//       separate method. Think about it or something.
	if cmd.Action == api.Give {
		n.UpdateRangeInfo(&api.RangeInfo{
			Meta:  p.Range().Meta,
			State: s,
			Info:  api.LoadInfo{},
		})
	} else {
		n.UpdateRangeState(p.Range().Meta.Ident, s)
	}

	return nil
}

func (a *Actuator) cmd(action api.Action, p *ranje.Placement, n *roster.Node) (api.RemoteState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	var s pb.RangeNodeState
	var err error

	switch action {
	case api.Give:
		s, err = give(ctx, n, p, util.GetParents(a.rg, a.ng, p.Range()))

	case api.Serve:
		s, err = serve(ctx, n, p)

	case api.Take:
		s, err = take(ctx, n, p)

	case api.Drop:
		s, err = drop(ctx, n, p)

	default:
		// TODO: Use exhaustive analyzer?
		panic(fmt.Sprintf("unknown action: %v", action))
	}

	if err != nil {
		return api.NsUnknown, err
	}

	return conv.RemoteStateFromProto(s), nil
}

func give(ctx context.Context, n *roster.Node, p *ranje.Placement, parents []*pb.Parent) (pb.RangeNodeState, error) {
	req := &pb.GiveRequest{
		Range:   conv.MetaToProto(p.Range().Meta),
		Parents: parents,
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Give(ctx, req)
	if err != nil {
		return pb.RangeNodeState_UNKNOWN, err
	}

	return res.RangeInfo.State, nil
}

func serve(ctx context.Context, n *roster.Node, p *ranje.Placement) (pb.RangeNodeState, error) {
	rID := p.Range().Meta.Ident
	req := &pb.ServeRequest{
		Range: conv.IdentToProto(rID),
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Serve(ctx, req)
	if err != nil {
		return pb.RangeNodeState_UNKNOWN, err
	}

	return res.State, nil
}

func take(ctx context.Context, n *roster.Node, p *ranje.Placement) (pb.RangeNodeState, error) {
	rID := p.Range().Meta.Ident
	req := &pb.TakeRequest{
		Range: conv.IdentToProto(rID),
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Take(ctx, req)
	if err != nil {
		return pb.RangeNodeState_UNKNOWN, err
	}

	return res.State, nil
}

func drop(ctx context.Context, n *roster.Node, p *ranje.Placement) (pb.RangeNodeState, error) {
	rID := p.Range().Meta.Ident
	req := &pb.DropRequest{
		Range: conv.IdentToProto(rID),
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Drop(ctx, req)
	if err != nil {
		return pb.RangeNodeState_UNKNOWN, err
	}

	return res.State, nil
}
