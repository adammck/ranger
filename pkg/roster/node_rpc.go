package roster

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

const rpcTimeout = 1 * time.Second

func (n *Node) Give(ctx context.Context, p *ranje.Placement) error {
	log.Printf("giving %s to %s...", p.LogString(), n.Ident())

	// TODO: Do something sensible when this is called while a previous Give is
	//       still in flight. Probably cancel the previous one first.

	// TODO: Include range parents
	req := &pb.GiveRequest{
		Range: p.Range().Meta.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Give(ctx, req)
	if err != nil {
		log.Printf("error giving %s to %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	// Parse the response, which contains the current state of the range.
	// TODO: This should only contain the remote state. We already know the
	//       meta, and the rest (usage info) is all probably zero at this point,
	//       and can be filled in by the next probe anyway.
	info, err := RangeInfoFromProto(res.RangeInfo)
	if err != nil {
		return fmt.Errorf("malformed probe response from %v: %v", n.Remote.Ident, err)
	}

	// Update the range info cache on the Node. This is faster than waiting for
	// the next probe, but is otherwise the same thing.
	func() {
		n.muRanges.Lock()
		defer n.muRanges.Unlock()
		n.ranges[info.Meta.Ident] = &info
	}()

	log.Printf("gave %s to %s; info=%v", p.LogString(), n.Ident(), info)
	return nil
}

func (n *Node) Serve(ctx context.Context, p *ranje.Placement) error {
	log.Printf("serving %s to %s...", p.LogString(), n.Ident())
	rID := p.Range().Meta.Ident

	// TODO: Include range parents
	req := &pb.ServeRequest{
		Range: rID.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Serve(ctx, req)
	if err != nil {
		log.Printf("error serving %s to %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	s := RemoteStateFromProto(res.State)

	// Update the state in the range info cache.
	func() {
		n.muRanges.Lock()
		defer n.muRanges.Unlock()
		ri, ok := n.ranges[rID]
		if ok {
			ri.State = s
		}
	}()

	log.Printf("served %s to %s; state=%v", p.LogString(), n.Ident(), s)
	return nil
}

func (n *Node) Take(ctx context.Context, p *ranje.Placement) error {
	log.Printf("taking %s from %s...", p.LogString(), n.Ident())
	rID := p.Range().Meta.Ident

	// TODO: Include range parents
	req := &pb.TakeRequest{
		Range: rID.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Take(ctx, req)
	if err != nil {
		log.Printf("error taking %s from %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	s := RemoteStateFromProto(res.State)

	// Update the state in the range info cache.
	func() {
		n.muRanges.Lock()
		defer n.muRanges.Unlock()
		ri, ok := n.ranges[rID]
		if ok {
			ri.State = s
		}
	}()

	log.Printf("took %s from %s; state=%v", p.LogString(), n.Ident(), s)
	return nil
}
