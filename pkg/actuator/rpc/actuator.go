package rpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/adammck/ranger/pkg/actuator/util"
	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/keyspace"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
	"github.com/adammck/ranger/pkg/roster/state"
)

type Actuator struct {
	ks  *keyspace.Keyspace
	ros *roster.Roster
	dup util.Deduper
}

const rpcTimeout = 1 * time.Second

func New(ks *keyspace.Keyspace, ros *roster.Roster) *Actuator {
	return &Actuator{
		ks:  ks,
		ros: ros,
		dup: util.NewDeduper(),
	}
}

// TODO: This is currently duplicated.
func (a *Actuator) Command(action api.Action, p *ranje.Placement, n *roster.Node) {
	a.dup.Exec(action, p, n, func() {
		s, err := a.cmd(action, p, n)
		if err != nil {
			log.Printf("actuation error: %v", err)
			return
		}

		// TODO: This special case is weird. It was less so when Give was a
		//       separate method. Think about it or something.
		if action == api.Give {
			n.UpdateRangeInfo(&api.RangeInfo{
				Meta:  p.Range().Meta,
				State: s,
				Info:  api.LoadInfo{},
			})
		} else {
			n.UpdateRangeState(p.Range().Meta.Ident, s)
		}
	})
}

func (a *Actuator) Wait() {
	a.dup.Wait()
}

func (a *Actuator) cmd(action api.Action, p *ranje.Placement, n *roster.Node) (api.RemoteState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	var s pb.RangeNodeState
	var err error

	switch action {
	case api.Give:
		s, err = give(ctx, n, p, util.GetParents(a.ks, a.ros, p.Range()))

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

	return state.RemoteStateFromProto(s), nil
}

func give(ctx context.Context, n *roster.Node, p *ranje.Placement, parents []*pb.Parent) (pb.RangeNodeState, error) {
	req := &pb.GiveRequest{
		Range:   ranje.MetaToProto(p.Range().Meta),
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
		Range: ranje.IdentToProto(rID),
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
		Range: ranje.IdentToProto(rID),
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
		Range: ranje.IdentToProto(rID),
	}

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Drop(ctx, req)
	if err != nil {
		return pb.RangeNodeState_UNKNOWN, err
	}

	return res.State, nil
}
