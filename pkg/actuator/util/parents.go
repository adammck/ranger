package util

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

// TODO: Where does this belong? Probably not here!
func GetParents(ks ranje.RangeGetter, ros roster.NodeGetter, rang *ranje.Range) []*pb.Parent {
	parents := []*pb.Parent{}
	seen := map[api.RangeID]struct{}{}
	addParents(ks, ros, rang, &parents, seen)
	return parents
}

func addParents(ks ranje.RangeGetter, ros roster.NodeGetter, rang *ranje.Range, parents *[]*pb.Parent, seen map[api.RangeID]struct{}) {
	_, ok := seen[rang.Meta.Ident]
	if ok {
		return
	}

	*parents = append(*parents, pbPlacement(ros, rang))
	seen[rang.Meta.Ident] = struct{}{}

	for _, rID := range rang.Parents {
		r, err := ks.GetRange(rID)
		if err != nil {
			// TODO: Think about how to recover from this. It's bad.
			panic(fmt.Sprintf("getting range with ident %v: %v", rID, err))
		}

		addParents(ks, ros, r, parents, seen)
	}
}

func pbPlacement(ros roster.NodeGetter, r *ranje.Range) *pb.Parent {

	// TODO: The kv example doesn't care about range history, because it has no
	// external write log, so can only fetch from nodes. So we can skip sending
	// them at all. Maybe add a controller feature flag?

	pbPlacements := make([]*pb.Placement, len(r.Placements))

	for i, p := range r.Placements {

		// TODO: Don't ignore errors here.
		n, _ := ros.NodeByIdent(p.NodeID)

		node := ""
		if n != nil {
			node = n.Addr()
		}

		pbPlacements[i] = &pb.Placement{
			Node:  node,
			State: conv.PlacementStateToProto(p.StateCurrent),
		}
	}

	return &pb.Parent{
		Range:      conv.MetaToProto(r.Meta),
		Placements: pbPlacements,
	}
}
