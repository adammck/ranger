package util

import (
	"fmt"

	"github.com/adammck/ranger/pkg/api"
	"github.com/adammck/ranger/pkg/proto/conv"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster"
)

// TODO: Move this somewhere else. It's useful in various places.
type RangeGetter interface {
	Get(id api.Ident) (*ranje.Range, error)
}

// TODO: NodeByIdent should probably return an error, not nil.
// TODO: Move this somewhere else. It's useful in various places.
type NodeGetter interface {
	NodeByIdent(nodeIdent string) *roster.Node
}

// TODO: Where does this belong? Probably not here!
func GetParents(ks RangeGetter, ros NodeGetter, rang *ranje.Range) []*pb.Parent {
	parents := []*pb.Parent{}
	seen := map[api.Ident]struct{}{}
	addParents(ks, ros, rang, &parents, seen)
	return parents
}

func addParents(ks RangeGetter, ros NodeGetter, rang *ranje.Range, parents *[]*pb.Parent, seen map[api.Ident]struct{}) {
	_, ok := seen[rang.Meta.Ident]
	if ok {
		return
	}

	*parents = append(*parents, pbPlacement(ros, rang))
	seen[rang.Meta.Ident] = struct{}{}

	for _, rID := range rang.Parents {
		r, err := ks.Get(rID)
		if err != nil {
			// TODO: Think about how to recover from this. It's bad.
			panic(fmt.Sprintf("getting range with ident %v: %v", rID, err))
		}

		addParents(ks, ros, r, parents, seen)
	}
}

func pbPlacement(ros NodeGetter, r *ranje.Range) *pb.Parent {

	// TODO: The kv example doesn't care about range history, because it has no
	//       external write log, so can only fetch from nodes. So we can skip
	//       sending them at all. Maybe add a controller feature flag?
	//

	pbPlacements := make([]*pb.Placement, len(r.Placements))

	for i, p := range r.Placements {
		n := ros.NodeByIdent(p.NodeID)

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
