package node

import (
	"log"

	"github.com/adammck/ranger/pkg/rangelet"
	"github.com/adammck/ranger/pkg/ranje"
)

// ---- control plane

func (n *Node) PrepareAddShard(rm ranje.Meta, parents []rangelet.Parent) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.data[rm.Ident]
	if ok {
		panic("rangelet gave duplicate range!")
	}

	// TODO: How to get parents in here?
	// if req.Parents != nil && len(req.Parents) > 0 {
	// 	rd.fetchMany(rm, req.Parents)
	// } else {
	// 	// TODO: Restore support for this:
	// 	// -- No current host nor parents. This is a brand new range. We're
	// 	// -- probably initializing a new empty keyspace.
	// 	// -- rd.state = state.NsReady
	// 	rd.state = state.NsPrepared
	// }

	n.data[rm.Ident] = &KeysVals{
		data:   map[string][]byte{},
		writes: false,
	}

	log.Printf("Given: %s", rm.Ident)
	return nil
}

func (n *Node) AddShard(rID ranje.Ident) error {
	// lol
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to serve unknown range!")
	}

	kv.writes = true

	log.Printf("Servng: %s", rID)
	return nil
}

func (n *Node) PrepareDropShard(rID ranje.Ident) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	kv, ok := n.data[rID]
	if !ok {
		panic("rangelet tried to serve unknown range!")
	}

	kv.writes = false

	log.Printf("Taking: %s", rID)
	return nil
}

// func (n *Node) Untake(ctx context.Context, req *pbr.UntakeRequest) (*pbr.UntakeResponse, error) {
// 	// lol
// 	s.node.mu.Lock()
// 	defer s.node.mu.Unlock()

// 	ident, rd, err := s.getRangeData(req.Range)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if rd.state != state.NsTaken {
// 		return nil, status.Error(codes.FailedPrecondition, "can only untake ranges in the TAKEN state")
// 	}

// 	rd.state = state.NsReady

// 	log.Printf("Untaken: %s", ident)
// 	return &pbr.UntakeResponse{}, nil
// }

func (n *Node) DropShard(rID ranje.Ident) error {
	// lol
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.data, rID)

	log.Printf("Dropped: %s", rID)
	return nil
}
