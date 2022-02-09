package ranje

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adammck/ranger/pkg/discovery"
	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

const (
	s             = time.Second
	staleTimer    = 10 * s
	giveTimeout   = 3 * s
	takeTimeout   = 3 * s
	untakeTimeout = 3 * s
	dropTimeout   = 3 * s
	serveTimeout  = 3 * s
)

type Node struct {
	remote discovery.Remote

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// time last seen in service discovery.
	seen time.Time

	conn   *grpc.ClientConn
	client pb.NodeClient
	muConn sync.RWMutex

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(remote discovery.Remote) *Node {
	n := Node{
		remote:           remote,
		init:             time.Now(),
		seen:             time.Time{}, // never
		unexpectedRanges: map[Ident]*pb.RangeMeta{},
	}

	// TODO: This is repeated in ShortNode now. Probably keep all conn stuff in there?

	// start dialling in background
	// todo: inherit context to allow global cancellation
	conn, err := grpc.DialContext(context.Background(), n.remote.Addr(), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("error while dialing: %v\n", err)
	}

	n.muConn.Lock()
	n.conn = conn
	n.client = pb.NewNodeClient(n.conn)
	n.muConn.Unlock()

	return &n
}

func (n *Node) Ident() string {
	return n.remote.Ident
}

func (n *Node) String() string {
	return fmt.Sprintf("N{%s}", n.remote.Ident)
}

// TODO: Replace this with a statusz-type page
func (n *Node) DumpForDebug() {
}

// UnsafeForgetPlacement removes the given placement from the ranges map of this
// node. Works by address, not value.
// TODO: Remove this method. It's useless now that nodes don't know placements.
func (n *Node) ForgetPlacement(p *DurablePlacement) error {
	return nil
}

// Seen tells us that the node is still in service discovery.
// TODO: Combine this with the ShortNode somehow? Maybe it's fine.
func (n *Node) Seen(t time.Time) {
	n.seen = t
}

// TODO: Combine this with the ShortNode somehow? Maybe it's fine.
func (n *Node) IsStale(now time.Time) bool {
	return n.seen.Before(now.Add(-staleTimer))
}

// Called by Placement to avoid leaking the node pointer.
func (n *Node) Take(p *DurablePlacement) error {

	// TODO: Move this into the callers; state is no business of Node.
	if p.state != SpReady {
		return fmt.Errorf("can't take range %s from node %s when state is %s",
			p.rang.String(), p.nodeID, p.state)
	}

	req := &pb.TakeRequest{
		// lol demeter who?
		Range: p.rang.Meta.Ident.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), takeTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := n.client.Take(ctx, req)
	if err != nil {
		// No state change. Placement is still SpReady.
		return err
	}

	return p.ToState(SpTaken)
}

func (n *Node) Untake(p *DurablePlacement) error {

	// TODO: Move this into the callers; state is no business of Node.
	if p.state != SpTaken {
		return fmt.Errorf("can't untake range %s from node %s when state is %s",
			p.rang.String(), p.nodeID, p.state)
	}

	req := &pb.UntakeRequest{
		Range: p.rang.Meta.Ident.ToProto(),
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), untakeTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := n.client.Untake(ctx, req)
	if err != nil {
		// No state change. Placement is still SpTaken.
		return err
	}

	// TODO: Also move this into caller?
	return p.ToState(SpReady)
}

func (n *Node) Drop(p *DurablePlacement) error {
	if p.state != SpTaken {
		return fmt.Errorf("can't drop range %s from node %s when state is %s",
			p.rang.String(), p.nodeID, p.state)
	}

	req := &pb.DropRequest{
		Range: p.rang.Meta.Ident.ToProto(),
		Force: false,
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := n.client.Drop(ctx, req)
	if err != nil {
		// No state change. Placement is still SpTaken.
		return err
	}

	return p.ToState(SpDropped)
}

func (n *Node) Serve(p *DurablePlacement) error {
	if p.nodeID != n.remote.Ident {
		return fmt.Errorf("mismatched nodeID: %s != %s",
			p.nodeID, n.remote.Ident)
	}

	if p.state != SpFetched {
		return fmt.Errorf("can't serve range %s from node %s when state is %s (wanted SpFetched)",
			p.rang.String(), p.nodeID, p.state)
	}

	req := &pb.ServeRequest{
		Range: p.rang.Meta.Ident.ToProto(),
		Force: false,
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), serveTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	_, err := n.client.Serve(ctx, req)
	if err != nil {
		// No state change. Placement is still SpFetched.
		return err
	}

	return p.ToState(SpReady)
}

func (n *Node) Give(p *DurablePlacement, req *pb.GiveRequest) error {
	if p.state != SpPending {
		return fmt.Errorf("can't give range %s to node %s when state is %s (wanted SpPending)",
			p.rang.String(), p.nodeID, p.state)
	}

	// TODO: Move outside this func?
	ctx, cancel := context.WithTimeout(context.Background(), giveTimeout)
	defer cancel()

	// TODO: Retry a few times before giving up.
	res, err := n.client.Give(ctx, req)
	if err != nil {
		return err
	}

	// TODO: Use updateLocalState here!
	// TODO: Check the return values of state changes or use MustState!
	rs := RemoteStateFromProto(res.State)
	if rs == StateReady {
		p.ToState(SpReady)

	} else if rs == StateFetching {
		p.ToState(SpFetching)

	} else if rs == StateFetched {
		// The fetch finished before the client returned.
		p.ToState(SpFetching)
		p.ToState(SpFetched)

	} else if rs == StateFetchFailed {
		// The fetch failed before the client returned.
		p.ToState(SpFetching)
		p.ToState(SpFetchFailed)

	} else {
		// Got either Unknown or Taken
		panic(fmt.Sprintf("unexpected remote state from Give: %s", rs.String()))
	}

	return nil
}

func updateLocalState(p *DurablePlacement, rns pb.RangeNodeState) {
	rs := RemoteStateFromProto(rns)
	switch rs {
	case StateUnknown:
		//fmt.Printf("Got range in unexpected state from node %s: %s\n", n.addr(), rr.String())

	case StateFetching:
		p.ToState(SpFetching)

	case StateFetched:
		if p.state == SpPending {
			p.ToState(SpFetching)
		}
		p.ToState(SpFetched)

	case StateFetchFailed:
		// TODO: Can the client provide any info about why this failed?
		if p.state == SpPending {
			p.ToState(SpFetching)
		}
		p.ToState(SpFetchFailed)

	case StateReady:
		p.ToState(SpReady)

	case StateTaken:
		p.ToState(SpTaken)
	}
}

// Probe updates current state of the node via RPC.
// Returns error if the RPC fails or if a probe is already in progess.
// TODO: This should probably call probe, and then return the results to some
//       other coordinator. Don't store loadinfo in the Node.
func (n *Node) Probe(ctx context.Context) error {
	// TODO: Abort if probe in progress.

	res, err := n.client.Info(ctx, &pb.InfoRequest{})
	if err != nil {
		fmt.Printf("Probe failed: %s\n", err)
		return err
	}

	for _, r := range res.Ranges {
		rr := r.Range
		if rr == nil {
			fmt.Printf("Malformed probe response from node %s: Range is nil\n", n.remote.Ident)
			continue
		}

		// id, err := IdentFromProto(rr.Ident)
		// if err != nil {
		// 	fmt.Printf("Got malformed ident from node %s: %s\n", n.addr(), err.Error())
		// 	continue
		// }

		// TODO: Move all of this outwards to some node state coordinator

		// p, ok := n.ranges[*id]

		// if !ok {
		// 	fmt.Printf("Got unexpected range from node %s: %s\n", n.addr(), id.String())
		// 	n.unexpectedRanges[*id] = rr
		// 	continue
		// }

		// updateLocalState(p, r.State)

		// TODO: We compare the Ident here even though we just fetched the assignment by ID. Is that... why
		// if !p.rang.SameMeta(*id, rr.Start, rr.End) {
		// 	fmt.Printf("Remote range did not match local range with same ident: %s\n", id.String())
		// 	continue
		// }

		// Finally update the remote info
		//p.K = r.Keys

		// TODO: Figure out wtf to do when remote state doesn't match local
		//rrr.state = RemoteStateFromProto(r.State)
	}

	return nil
}

func (n *Node) Conn() (grpc.ClientConnInterface, error) {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	if n.conn == nil {
		return nil, errors.New("tried to read nil connection")
	}
	return n.conn, nil
}

func (n *Node) Connected() bool {
	n.muConn.RLock()
	defer n.muConn.RUnlock()
	return n.conn != nil
}
