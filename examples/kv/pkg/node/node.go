package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	pbr "github.com/adammck/ranger/pkg/proto/gen"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type key []byte

// wraps ranger/pkg/proto/gen/Ident
// TODO: move this to the lib
type rangeIdent [40]byte

// TODO: move this to the lib
func parseIdent(pbid *pbr.Ident) (rangeIdent, error) {
	ident := [40]byte{}

	s := []byte(pbid.GetScope())
	if len(s) > 32 {
		return ident, errors.New("invalid range ident: scope too long")
	}

	copy(ident[:], s)
	binary.LittleEndian.PutUint64(ident[32:], pbid.GetKey())

	return rangeIdent(ident), nil
}

// TODO: move this to the lib
func (i rangeIdent) String() string {
	scope, key := i.Decode()

	if scope == "" {
		return fmt.Sprintf("%d", key)
	}

	return fmt.Sprintf("%s/%d", scope, key)
}

// this is only necessary because I made the Dump interface friendly. it would probably be simpler to accept an encoded range ident, or maybe do a better job of hiding the [40]byte
func (i rangeIdent) Decode() (string, uint64) {
	scope := string(bytes.TrimRight(i[:32], "\x00"))
	key := binary.LittleEndian.Uint64(i[32:])
	return scope, key
}

// See also pb.RangeMeta.
type RangeMeta struct {
	ident rangeIdent
	start []byte
	end   []byte
}

func parseRangeMeta(r *pbr.Range) (RangeMeta, error) {
	ident, err := parseIdent(r.Ident)
	if err != nil {
		return RangeMeta{}, err
	}

	return RangeMeta{
		ident: ident,
		start: r.Start,
		end:   r.End,
	}, nil
}

func (rm *RangeMeta) Contains(k key) bool {
	return bytes.Compare(k, rm.start) >= 0 && bytes.Compare(k, rm.end) < 0
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

func (rs *Ranges) Remove(ident rangeIdent) {
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

func (rs *Ranges) Find(k key) (rangeIdent, bool) {
	for _, rm := range rs.ranges {
		if rm.Contains(k) {
			return rm.ident, true
		}
	}

	return rangeIdent{}, false
}

type RangeState uint8

const (
	rsUnknown RangeState = iota
	rsFetching
	rsFetched
	rsFetchFailed
	rsReady
	rsTaken
)

// This is all specific to the kv example. Nothing generic in here.
type RangeData struct {
	data  map[string][]byte
	state RangeState // TODO: guard this
}

func (rd *RangeData) fetchMany(dest RangeMeta, parents []*pbr.RangeNode) {

	// Parse all the parents before spawning threads. This is fast and failure
	// indicates a bug more than a transient problem.
	rms := make([]*RangeMeta, len(parents))
	for i, p := range parents {
		rm, err := parseRangeMeta(p.Range)
		if err != nil {
			log.Printf("FetchMany failed fast: %s", err)
			rd.state = rsFetchFailed
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
		// lol, golang
		// https://golang.org/doc/faq#closures_and_goroutines
		i := i

		g.Go(func() error {
			return rd.fetchOne(ctx, &mu, dest, parents[i].Node, rms[i])
		})
	}

	if err := g.Wait(); err != nil {
		rd.state = rsFetchFailed
		return
	}

	// Can't go straight into rsReady, because that allows writes. The source
	// node(s) are still serving reads, and if we start writing, they'll be
	// wrong. We can only serve reads until the assigner tells them to stop,
	// which will redirect all reads to us. Then we can start writing.
	rd.state = rsFetched
}

func (rd *RangeData) fetchOne(ctx context.Context, mu *sync.Mutex, dest RangeMeta, addr string, src *RangeMeta) error {
	log.Printf("FetchOne: %s from: %s", src.ident, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	client := pbkv.NewKVClient(conn)

	scope, key := src.ident.Decode()
	res, err := client.Dump(ctx, &pbkv.DumpRequest{Range: &pbkv.Ident{Scope: scope, Key: key}})
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
	data   map[rangeIdent]*RangeData
	ranges Ranges
	mu     sync.Mutex // guards data and ranges, todo: split into one for ranges, and one for each range in data

	// Probably okay to move this into the lib and use grpc health-checking every time.
	hs *health.Server

	// Don't always want Consul for discovery though. Need to abstract this.
	ca *consul.Agent
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
		if rd.state == rsFetchFailed {
			delete(n.node.data, rm.ident)
			n.node.ranges.Remove(rm.ident)
		} else {
			return nil, fmt.Errorf("already have ident: %s", rm.ident)
		}
	}

	rd = &RangeData{
		data:  make(map[string][]byte),
		state: rsUnknown,
	}

	if req.Parents != nil && len(req.Parents) > 0 {
		go rd.fetchMany(rm, req.Parents)

	} else {
		// No current host nor parents. This is a brand new range. We're
		// probably initializing a new empty scope.
		rd.state = rsReady
	}

	n.node.ranges.Add(rm)
	n.node.data[rm.ident] = rd

	log.Printf("Given: %s", rm.ident)
	return &pbr.GiveResponse{}, nil
}

func (s *nodeServer) Serve(ctx context.Context, req *pbr.ServeRequest) (*pbr.ServeResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	if rd.state != rsFetched && !req.Force {
		return nil, status.Error(codes.Aborted, "won't serve ranges not in the FETCHED state without FORCE")
	}

	rd.state = rsReady

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

	if rd.state != rsReady {
		return nil, status.Error(codes.FailedPrecondition, "can only take ranges in the READY state")
	}

	rd.state = rsTaken

	log.Printf("Taken: %s", ident)
	return &pbr.TakeResponse{}, nil
}

func (s *nodeServer) Drop(ctx context.Context, req *pbr.DropRequest) (*pbr.DropResponse, error) {
	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident, rd, err := s.getRangeData(req.Range)
	if err != nil {
		return nil, err
	}

	// Skipping this for now; we'll need to cancel via a context in rd.
	if rd.state == rsFetching {
		return nil, status.Error(codes.Unimplemented, "dropping ranges in the FETCHING state is not supported yet")
	}

	if rd.state != rsTaken && !req.Force {
		return nil, status.Error(codes.Aborted, "won't drop ranges not in the TAKEN state without FORCE")
	}

	delete(s.node.data, ident)
	s.node.ranges.Remove(ident)

	log.Printf("Dropped: %s", ident)
	return &pbr.DropResponse{}, nil
}

// Does not lock range map! You have do to that!
func (s *nodeServer) getRangeData(pbi *pbr.Ident) (rangeIdent, *RangeData, error) {
	if pbi == nil {
		return rangeIdent{}, nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	ident, err := parseIdent(pbi)
	if err != nil {
		return ident, nil, status.Errorf(codes.InvalidArgument, "error parsing range ident: %v", err)
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
	r := req.Range
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	// TODO: Import the proto properly instead of casting like this!
	ident, err := parseIdent(&pbr.Ident{
		Scope: req.Range.Scope,
		Key:   req.Range.Key,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing range ident: %v", err)
	}

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	rd, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	if rd.state != rsTaken {
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

	if rd.state != rsReady && rd.state != rsFetched && rd.state != rsTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only GET from ranges in the READY, FETCHED, and TAKEN states")
	}

	v, ok := rd.data[k]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such key")
	}

	log.Printf("get %q", k)
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

	if rd.state != rsReady {
		return nil, status.Error(codes.FailedPrecondition, "can only PUT to ranges in the READY state")
	}

	if req.Value == nil {
		delete(rd.data, k)
	} else {
		rd.data[k] = req.Value
	}

	log.Printf("put %q", k)
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

func Build() (*Node, *grpc.Server) {
	n := &Node{
		data:   make(map[rangeIdent]*RangeData),
		ranges: NewRanges(),
		hs:     health.NewServer(),
	}

	ns := nodeServer{node: n}
	kv := kvServer{node: n}

	var opts []grpc.ServerOption
	srv := grpc.NewServer(opts...)

	pbr.RegisterNodeServer(srv, &ns)
	pbkv.RegisterKVServer(srv, &kv)

	// start healthy?
	n.hs.SetServingStatus("", hv1.HealthCheckResponse_SERVING)
	hv1.RegisterHealthServer(srv, n.hs)

	// Register reflection service, so client can introspect (for debugging).
	reflection.Register(srv)

	return n, srv
}

func join(node *Node, addrPub string) error {

	// Add Consul agent so controller can find this node.

	cc, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return err
	}
	node.ca = cc.Agent()

	// TODO: Move this outwards?
	host, sPort, err := net.SplitHostPort(addrPub)
	if err != nil {
		panic(err)
	}
	nPort, err := strconv.Atoi(sPort)
	if err != nil {
		panic(err)
	}
	fmt.Printf("addrPub: host=%+v, port=%+v\n", host, nPort)

	def := &consul.AgentServiceRegistration{
		Name:    "node",
		ID:      fmt.Sprintf("node-%d", os.Getpid()),
		Address: host,
		Port:    nPort,

		Check: &consul.AgentServiceCheck{
			GRPC:     addrPub, //"localhost:9000",
			Interval: (3 * time.Second).String(),
			Timeout:  (10 * time.Second).String(),
		},
	}

	err = node.ca.ServiceRegister(def)
	if err != nil {
		return err
	}

	return nil
}

func Start(addrLis, addrPub string) error {
	node, srv := Build()

	lis, err := net.Listen("tcp", addrLis)
	if err != nil {
		return err
	}

	err = join(node, addrPub)
	if err != nil {
		return err
	}

	log.Printf("listening on: %s", addrLis)
	return srv.Serve(lis)
}
