package node

import (
	"bytes"
	"context"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/adammck/ranger/pkg/config"
	"github.com/adammck/ranger/pkg/discovery"
	consuldisc "github.com/adammck/ranger/pkg/discovery/consul"
	pbr "github.com/adammck/ranger/pkg/proto/gen"
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/state"
	consulapi "github.com/hashicorp/consul/api"
)

type key []byte

// See also pb.RangeMeta.
// TODO: Replace with ranje.Meta!
type RangeMeta struct {
	ident ranje.Ident
	start []byte
	end   []byte
}

func parseRangeMeta(r *pbr.RangeMeta) (RangeMeta, error) {
	rID, err := ranje.IdentFromProto(r.Ident)
	if err != nil {
		return RangeMeta{}, err
	}

	return RangeMeta{
		ident: rID,
		start: r.Start,
		end:   r.End,
	}, nil
}

func (rm *RangeMeta) Contains(k key) bool {
	return ((ranje.Key(rm.start) == ranje.ZeroKey || bytes.Compare(k, rm.start) >= 0) &&
		(ranje.Key(rm.end) == ranje.ZeroKey || bytes.Compare(k, rm.end) < 0))
}

// Doesn't have a mutex, since that probably happens outside, to synchronize with other structures.
type Ranges struct {
	ranges []RangeMeta
}

func NewRanges() Ranges {
	return Ranges{ranges: make([]RangeMeta, 0)}
}

func (rs *Ranges) Add(r RangeMeta) error {
	rs.ranges = append(rs.ranges, r)
	return nil
}

func (rs *Ranges) Remove(ident ranje.Ident) {
	idx := -1

	for i := range rs.ranges {
		if rs.ranges[i].ident == ident {
			idx = i
			break
		}
	}

	// This REALLY SHOULD NOT happen, because the ident should have come
	// straight of the range map, and we should still be under the same lock.
	if idx == -1 {
		panic("ident not found in range map")
	}

	// jfc golang
	// https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order
	rs.ranges[idx] = rs.ranges[len(rs.ranges)-1]
	rs.ranges = rs.ranges[:len(rs.ranges)-1]
}

func (rs *Ranges) Find(k key) (ranje.Ident, bool) {
	for _, rm := range rs.ranges {
		if rm.Contains(k) {
			return rm.ident, true
		}
	}

	return ranje.ZeroRange, false
}

// Len returns the number of ranges this node has, in any state.
func (rs *Ranges) Len() int {
	return len(rs.ranges)
}

// This is all specific to the kv example. Nothing generic in here.
type RangeData struct {
	data map[string][]byte

	// TODO: Move this to the rangemeta!!
	state state.RemoteState // TODO: guard this
}

func (rd *RangeData) fetchMany(dest RangeMeta, parents []*pbr.Placement) {

	// Parse all the parents before spawning threads. This is fast and failure
	// indicates a bug more than a transient problem.
	rms := make([]*RangeMeta, len(parents))
	for i, p := range parents {
		rm, err := parseRangeMeta(p.Range)
		if err != nil {
			log.Printf("FetchMany failed fast: %s", err)
			rd.state = state.NsPreparingError
			return
		}
		rms[i] = &rm
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mu := sync.Mutex{}

	// Fetch each source range in parallel.
	g, ctx := errgroup.WithContext(ctx)
	for i := range parents {

		// Ignore ranges which aren't currently assigned to any node. This kv
		// example doesn't have an external write log, so if it isn't placed,
		// there's nothing we can do with it.
		//
		// This includes ranges which are being placed for the first time, which
		// includes new split/join ranges! That isn't an error. Their state is
		// reassembled from their parents.
		if parents[i].Node == "" {
			//log.Printf("not fetching unplaced range: %s", rms[i].ident)
			continue
		}

		// lol, golang
		// https://golang.org/doc/faq#closures_and_goroutines
		i := i

		g.Go(func() error {
			return rd.fetchOne(ctx, &mu, dest, parents[i].Node, rms[i])
		})
	}

	if err := g.Wait(); err != nil {
		rd.state = state.NsPreparingError
		return
	}

	// Can't go straight into rsReady, because that allows writes. The source
	// node(s) are still serving reads, and if we start writing, they'll be
	// wrong. We can only serve reads until the assigner tells them to stop,
	// which will redirect all reads to us. Then we can start writing.
	rd.state = state.NsPrepared
}

func (rd *RangeData) fetchOne(ctx context.Context, mu *sync.Mutex, dest RangeMeta, addr string, src *RangeMeta) error {
	if addr == "" {
		log.Printf("FetchOne: %s with no addr", src.ident)
		return nil
	}

	log.Printf("FetchOne: %s from: %s", src.ident, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		// TODO: Probably a bit excessive
		log.Fatalf("fail to dial: %v", err)
	}

	client := pbkv.NewKVClient(conn)

	res, err := client.Dump(ctx, &pbkv.DumpRequest{RangeIdent: uint64(src.ident)})
	if err != nil {
		log.Printf("FetchOne failed: %s from: %s: %s", src.ident, addr, err)

		return err
	}

	// TODO: Optimize loading by including range start and end in the Dump response. If they match, can skip filtering.

	c := 0
	s := 0
	mu.Lock()
	for _, pair := range res.Pairs {

		// TODO: Untangle []byte vs string mess
		if dest.Contains(pair.Key) {
			rd.data[string(pair.Key)] = pair.Value
			c += 1
		} else {
			s += 1
		}
	}
	mu.Unlock()
	log.Printf("Inserted %d keys from range %s via node %s (skipped %d)", c, src.ident, addr, s)

	return nil
}

type Node struct {
	cfg config.Config

	data   map[ranje.Ident]*RangeData
	ranges Ranges
	mu     sync.RWMutex // guards data and ranges, todo: split into one for ranges, and one for each range in data

	addrLis string
	addrPub string
	srv     *grpc.Server
	disc    discovery.Discoverable

	// Set by Node.DrainRanges.
	wantDrain bool

	// Options
	logReqs bool
}

// ---- control plane

type nodeServer struct {
	pbr.UnimplementedNodeServer
	node *Node
}

// TODO: most of this can be moved into the lib?
func (n *nodeServer) Give(ctx context.Context, req *pbr.GiveRequest) (*pbr.GiveResponse, error) {
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rm, err := parseRangeMeta(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// TODO: Look in Ranges instead here?
	rd, ok := n.node.data[rm.ident]
	if ok {
		// Special case: We already have this range, but gave up on fetching it.
		// To keep things simple, delete it. it'll be added again (while still
		// holding the lock) below.
		if rd.state == state.NsPreparingError {
			delete(n.node.data, rm.ident)
			n.node.ranges.Remove(rm.ident)

		} else {

			log.Printf("Redundant Give: %s", rm.ident)
			return &pbr.GiveResponse{
				RangeInfo: &pbr.RangeInfo{
					Meta:  r,
					State: rd.state.ToProto(),
				},
			}, nil

		}
	}

	rd = &RangeData{
		data:  make(map[string][]byte),
		state: state.NsUnknown, // default
	}

	if req.Parents != nil && len(req.Parents) > 0 {
		rd.state = state.NsPreparing
		rd.fetchMany(rm, req.Parents)

	} else {

		// TODO: Restore support for this:
		// -- No current host nor parents. This is a brand new range. We're
		// -- probably initializing a new empty keyspace.
		// -- rd.state = state.NsReady

		// Temporary
		rd.state = state.NsPrepared

	}

	n.node.ranges.Add(rm)
	n.node.data[rm.ident] = rd

	log.Printf("Given: %s", rm.ident)
	return &pbr.GiveResponse{
		RangeInfo: &pbr.RangeInfo{
			Meta:  r,
			State: rd.state.ToProto(),
		},
	}, nil
}

func (s *nodeServer) Serve(ctx context.Context, req *pbr.ServeRequest) (*pbr.ServeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != state.NsPrepared && !req.Force {
		return nil, status.Error(codes.Aborted, "won't serve ranges not in the FETCHED state without FORCE")
	}

	rd.state = state.NsReady

	log.Printf("Serving: %s", ident)
	return &pbr.ServeResponse{}, nil
}

func (s *nodeServer) Take(ctx context.Context, req *pbr.TakeRequest) (*pbr.TakeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != state.NsReady {
		return nil, status.Error(codes.FailedPrecondition, "can only take ranges in the READY state")
	}

	rd.state = state.NsTaken

	log.Printf("Taken: %s", ident)
	return &pbr.TakeResponse{}, nil
}

func (s *nodeServer) Untake(ctx context.Context, req *pbr.UntakeRequest) (*pbr.UntakeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != state.NsTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only untake ranges in the TAKEN state")
	}

	rd.state = state.NsReady

	log.Printf("Untaken: %s", ident)
	return &pbr.UntakeResponse{}, nil
}

func (s *nodeServer) Drop(ctx context.Context, req *pbr.DropRequest) (*pbr.DropResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident := ranje.Ident(req.Range)
	if ident == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		log.Printf("got redundant Drop (no such range; maybe drop complete)")

		// This is NOT a failure.
		return &pbr.DropResponse{
			State: state.NsNotFound.ToProto(),
		}, nil
	}

	// Skipping this for now; we'll need to cancel via a context in rd.
	if rd.state == state.NsPreparing {
		return nil, status.Error(codes.Unimplemented, "dropping ranges in the FETCHING state is not supported yet")
	}

	if rd.state != state.NsTaken && !req.Force {
		return nil, status.Error(codes.Aborted, "won't drop ranges not in the TAKEN state without FORCE")
	}

	delete(s.node.data, ident)
	s.node.ranges.Remove(ident)

	log.Printf("Dropped: %s", ident)
	return &pbr.DropResponse{
		State: pbr.RangeNodeState_NOT_FOUND,
	}, nil
}

func (n *nodeServer) Info(ctx context.Context, req *pbr.InfoRequest) (*pbr.InfoResponse, error) {
	res := &pbr.InfoResponse{
		WantDrain: n.node.wantDrain,
	}

	// lol
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// iterate range metadata
	for _, r := range n.node.ranges.ranges {
		d := n.node.data[r.ident]

		res.Ranges = append(res.Ranges, &pbr.RangeInfo{
			Meta: &pbr.RangeMeta{
				Ident: uint64(r.ident),
				Start: r.start,
				End:   r.end,
			},
			State: d.state.ToProto(),
			Keys:  uint64(len(d.data)),
		})
	}

	return res, nil
}

func (n *nodeServer) Ranges(ctx context.Context, req *pbr.RangesRequest) (*pbr.RangesResponse, error) {
	res := &pbr.RangesResponse{}

	// lol
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// TODO: Filter out ranges based on req.Symbols (e.g. KV.Get)

	for _, r := range n.node.ranges.ranges {
		d := n.node.data[r.ident]

		res.Ranges = append(res.Ranges, &pbr.RangeMetaState{
			Meta: &pbr.RangeMeta{
				Ident: uint64(r.ident),
				// Empty when infinity
				Start: r.start,
				End:   r.end,
			},

			// TODO: This belongs in the RangeMeta.
			State: d.state.ToProto(),
		})
	}

	return res, nil
}

// Does not lock range map! You have do to that!
func (s *nodeServer) getRangeData(pbi uint64) (ranje.Ident, *RangeData, error) {
	ident := ranje.Ident(pbi)
	if ident == 0 {
		return ranje.ZeroRange, nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		return ident, nil, status.Error(codes.InvalidArgument, "range not found")
	}

	return ident, rd, nil
}

// ---- data plane

type kvServer struct {
	pbkv.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pbkv.DumpRequest) (*pbkv.DumpResponse, error) {
	ident := ranje.Ident(req.RangeIdent)
	if ident == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing: range_ident")
	}

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	rd, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	if rd.state != state.NsTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only dump ranges in the TAKEN state")
	}

	res := &pbkv.DumpResponse{}
	for k, v := range rd.data {
		res.Pairs = append(res.Pairs, &pbkv.Pair{Key: []byte(k), Value: v})
	}

	log.Printf("Dumped: %s", ident)
	return res, nil
}

func (s *kvServer) Get(ctx context.Context, req *pbkv.GetRequest) (*pbkv.GetResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.ranges.Find(key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if rd.state != state.NsReady && rd.state != state.NsPrepared && rd.state != state.NsTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only GET from ranges in the READY, FETCHED, and TAKEN states")
	}

	v, ok := rd.data[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	if s.node.logReqs {
		log.Printf("get %q", k)
	}

	return &pbkv.GetResponse{
		Value: v,
	}, nil
}

func (s *kvServer) Put(ctx context.Context, req *pbkv.PutRequest) (*pbkv.PutResponse, error) {
	k := string(req.Key)
	if k == "" {
		return nil, status.Error(codes.InvalidArgument, "missing: key")
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, ok := s.node.ranges.Find(key(k))
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "no valid range")
	}

	rd, ok := s.node.data[ident]
	if !ok {
		panic("range found in map but no data?!")
	}

	if rd.state != state.NsReady {
		return nil, status.Error(codes.FailedPrecondition, "can only PUT to ranges in the READY state")
	}

	if req.Value == nil {
		delete(rd.data, k)
	} else {
		rd.data[k] = req.Value
	}

	if s.node.logReqs {
		log.Printf("put %q", k)
	}

	return &pbkv.PutResponse{}, nil
}

func init() {
	// Ensure that nodeServer implements the NodeServer interface
	var ns *nodeServer = nil
	var _ pbr.NodeServer = ns

	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pbkv.KVServer = kvs

}

func New(cfg config.Config, addrLis, addrPub string, logReqs bool) (*Node, error) {
	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	// Register reflection service, so client can introspect (for debugging).
	// TODO: Make this optional.
	reflection.Register(srv)

	disc, err := consuldisc.New("node", addrPub, consulapi.DefaultConfig(), srv)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:    cfg,
		data:   make(map[ranje.Ident]*RangeData),
		ranges: NewRanges(),

		addrLis: addrLis,
		addrPub: addrPub,
		srv:     srv,
		disc:    disc,

		logReqs: logReqs,
	}

	ns := nodeServer{node: n}
	kv := kvServer{node: n}

	pbr.RegisterNodeServer(srv, &ns)
	pbkv.RegisterKVServer(srv, &kv)

	return n, nil
}

func (n *Node) Run(ctx context.Context) error {

	// For the gRPC server.
	lis, err := net.Listen("tcp", n.addrLis)
	if err != nil {
		return err
	}

	log.Printf("listening on: %s", n.addrLis)

	// Start the gRPC server in a background routine.
	errChan := make(chan error)
	go func() {
		err := n.srv.Serve(lis)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	// Register with service discovery
	err = n.disc.Start()
	if err != nil {
		return err
	}

	// Block until context is cancelled, indicating that caller wants shutdown.
	<-ctx.Done()

	// We're shutting down. First drain all of the ranges off this node (which
	// may take a while and involve a bunch of RPCs.) If this is disabled, the
	// node will just disappear, and the controller will wait until it expires
	// before assigning the range to other nodes.
	if n.cfg.DrainNodesBeforeShutdown {
		n.DrainRanges()
	} else {
		log.Printf("not draining ranges")
	}

	// Let in-flight RPCs finish and then stop. errChan will contain the error
	// returned by srv.Serve (above) or be closed with no error.
	n.srv.GracefulStop()
	err = <-errChan
	if err != nil {
		log.Printf("error from srv.Serve: %v", err)
		return err
	}

	// Remove ourselves from service discovery. Not strictly necessary, but lets
	// the other nodes respond quicker.
	err = n.disc.Stop()
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) DrainRanges() {
	log.Printf("draining ranges...")

	// This is included in probe responses. The next time the controller probes
	// this node, it will notice that the node wants to drain (probably because
	// it's shutting down), and start removing ranges.
	n.wantDrain = true

	// Log the number of ranges remaining every five seconds while waiting.

	tick := time.NewTicker(5 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				n.mu.RLock()
				c := n.ranges.Len()
				n.mu.RUnlock()

				log.Printf("ranges remaining: %d", c)
			}
		}
	}()

	// Block until the number of ranges hits zero.

	for {
		n.mu.RLock()
		c := n.ranges.Len()
		n.mu.RUnlock()

		if c == 0 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Stop the logger.

	tick.Stop()
	done <- true

	log.Printf("finished draining ranges.")
}
