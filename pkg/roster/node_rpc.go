package roster

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
)

const giveTimeout = 1 * time.Second

func (n *Node) Give(ctx context.Context, p *ranje.Placement) error {
	log.Printf("giving %s to %s...", p.LogString(), n.Ident())

	// TODO: Do something sensible when this is called while a previous Give is
	//       still in flight. Probably cancel the previous one first.

	// TODO: Include range parents
	req := &pb.GiveRequest{
		Range: p.Range().Meta.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(ctx, giveTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.Client.Give(ctx, req)
	if err != nil {
		log.Printf("error giving %s to %s: %v", p.LogString(), n.Ident(), err)
		return err
	}

	// Parse the response, which contains the current state of the range.
	info, err := RangeInfoFromProto(res.RangeInfo)
	if err != nil {
		return fmt.Errorf("malformed probe response from %v: %v", n.Remote.Ident, err)
	}

	// Update the range info cache on the Node. This is faster than waiting for
	// the next probe, but is otherwise the same thing.
	n.muRanges.Lock()
	n.ranges[info.Meta.Ident] = info
	n.muRanges.Unlock()

	log.Printf("gave %s to %s; info=%v", p.LogString(), n.Ident(), info)
	return nil
}
