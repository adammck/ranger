package ranje

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/adammck/ranger/pkg/proto/gen"
	"google.golang.org/grpc"
)

const (
	staleTimer   = 10 * time.Second
	giveTimeout  = 3 * time.Second
	takeTimeout  = 3 * time.Second
	dropTimeout  = 3 * time.Second
	serveTimeout = 3 * time.Second
)

// TODO: Add the ident in here?
type Node struct {
	host string
	port int

	// when was this created? needed to drop nodes which never connect.
	init time.Time

	// time last seen in service discovery.
	seen time.Time

	conn   *grpc.ClientConn
	client pb.NodeClient
	muConn sync.RWMutex

	// The ranges that this node has.
	// TODO: Should this skip the placement and go straight to the Range?
	ranges   map[Ident]*Placement
	muRanges sync.RWMutex

	// TODO: Figure out what to do with these. They shouldn't exist, and indicate a state bug. But ignoring them probably isn't right.
	unexpectedRanges map[Ident]*pb.RangeMeta
}

func NewNode(host string, port int) *Node {
	n := Node{
		host:             host,
		port:             port,
		init:             time.Now(),
		seen:             time.Time{}, // never
		ranges:           map[Ident]*Placement{},
		unexpectedRanges: map[Ident]*pb.RangeMeta{},
	}

	// TODO: This is repeated in ShortNode now. Probably keep all conn stuff in there?

	// start dialling in background
	// todo: inherit context to allow global cancellation
	conn, err := grpc.DialContext(context.Background(), fmt.Sprintf("%s:%d", n.host, n.port), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("error while dialing: %v\n", err)
	}

	n.muConn.Lock()
	n.conn = conn
	n.client = pb.NewNodeClient(n.conn)
	n.muConn.Unlock()

	return &n
}

func (n *Node) String() string {
	return fmt.Sprintf("N{%s}", n.addr())
}

// TODO: Replace this with a statusz-type page
func (n *Node) DumpForDebug() {
	for id, p := range n.ranges {
		fmt.Printf("   - %s %s\n", id.String(), p.state.String())
	}
}

// UnsafeForgetPlacement removes the given placement from the ranges map of this
// node. Works by address, not value. The caller must hold muRanges for writing.
func (n *Node) UnsafeForgetPlacement(p *Placement) error {
	for id, p_ := range n.ranges {
		if p == p_ {
			delete(n.ranges, id)
			return nil
		}
	}

	return fmt.Errorf("couldn't forget placement of range %s on node %s; not found",
		p.rang.String(), n.String())
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

// TODO: Maybe replace host/port with a discovery.Remote and move this there?
func (n *Node) addr() string {
	return fmt.Sprintf("%s:%d", n.host, n.port)
}

// Called by Placement to avoid leaking the node pointer.
func (n *Node) take(p *Placement) error {
	if p.state != SpReady {
		return fmt.Errorf("can't take range %s from node %s when state is %s",
			p.rang.String(), p.node.String(), p.state)
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

func (n *Node) drop(p *Placement) error {
	if p.state != SpTaken {
		return fmt.Errorf("can't drop range %s from node %s when state is %s",
			p.rang.String(), p.node.String(), p.state)
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

func (n *Node) serve(p *Placement) error {
	if p.state != SpFetched {
		return fmt.Errorf("can't serve range %s from node %s when state is %s (wanted SpFetched)",
			p.rang.String(), p.node.String(), p.state)
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

func (n *Node) give(p *Placement, req *pb.GiveRequest) error {
	if p.state != SpPending {
		return fmt.Errorf("can't serve range %s from node %s when state is %s (wanted SpPending)",
			p.rang.String(), p.node.String(), p.state)
	}

	// TODO: Is there any point in this?
	_, ok := n.ranges[p.rang.Meta.Ident]
	if !ok {
		panic("give called but placement not in node ranges!")
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

func updateLocalState(p *Placement, rns pb.RangeNodeState) {
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
			fmt.Printf("Malformed probe response from node %s: Range is nil\n", n.addr())
			continue
		}

		id, err := IdentFromProto(rr.Ident)
		if err != nil {
			fmt.Printf("Got malformed ident from node %s: %s\n", n.addr(), err.Error())
			continue
		}

		p, ok := n.ranges[*id]

		if !ok {
			fmt.Printf("Got unexpected range from node %s: %s\n", n.addr(), id.String())
			n.unexpectedRanges[*id] = rr
			continue
		}

		updateLocalState(p, r.State)

		// TODO: We compare the Ident here even though we just fetched the assignment by ID. Is that... why
		if !p.rang.SameMeta(*id, rr.Start, rr.End) {
			fmt.Printf("Remote range did not match local range with same ident: %s\n", id.String())
			continue
		}

		// Finally update the remote info
		p.K = r.Keys

		// TODO: Figure out wtf to do when remote state doesn't match local
		//rrr.state = RemoteStateFromProto(r.State)
	}

	return nil
}

// Drop cleans up the node. Called when it hasn't responded to probes in a long time.
func (n *Node) Drop() {
	n.muConn.Lock()
	defer n.muConn.Unlock()
	n.conn.Close()
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
