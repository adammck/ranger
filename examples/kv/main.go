package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb2 "github.com/adammck/ranger/examples/kv/proto/gen"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type key []byte

// wraps ranger/pkg/proto/gen/Ident
// TODO: move this to the lib
type rangeIdent [40]byte

// TODO: move this to the lib
func parseIdent(pbid *pb.Ident) (rangeIdent, error) {
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

func parseRangeMeta(pbr *pb.Range) (RangeMeta, error) {
	ident, err := parseIdent(pbr.Ident)
	if err != nil {
		return RangeMeta{}, err
	}

	return RangeMeta{
		ident: ident,
		start: pbr.Start,
		end:   pbr.End,
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

func (rd *RangeData) fetchMany(dest RangeMeta, parents []*pb.RangeNode) {

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

	client := pb2.NewKVClient(conn)

	scope, key := src.ident.Decode()
	res, err := client.Dump(ctx, &pb2.DumpRequest{Range: &pb2.Ident{Scope: scope, Key: key}})
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
		if dest.Contains([]byte(pair.Key)) {
			rd.data[pair.Key] = pair.Value
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
}

// ---- control plane

type nodeServer struct {
	pb.UnimplementedNodeServer
	node *Node
}

// TODO: most of this can be moved into the lib?
func (n *nodeServer) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	pbr := req.Range
	if pbr == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	rm, err := parseRangeMeta(pbr)
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
	return &pb.GiveResponse{}, nil
}

func (s *nodeServer) Serve(ctx context.Context, req *pb.ServeRequest) (*pb.ServeResponse, error) {
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
	return &pb.ServeResponse{}, nil
}

func (s *nodeServer) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
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
	return &pb.TakeResponse{}, nil
}

func (s *nodeServer) Drop(ctx context.Context, req *pb.DropRequest) (*pb.DropResponse, error) {
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
	return &pb.DropResponse{}, nil
}

// Does not lock range map! You have do to that!
func (s *nodeServer) getRangeData(pbr *pb.Ident) (rangeIdent, *RangeData, error) {
	if pbr == nil {
		return rangeIdent{}, nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	ident, err := parseIdent(pbr)
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
	pb2.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pb2.DumpRequest) (*pb2.DumpResponse, error) {
	pbr := req.Range
	if pbr == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	// TODO: Import the proto properly instead of casting like this!
	ident, err := parseIdent(&pb.Ident{
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

	res := &pb2.DumpResponse{}
	for k, v := range rd.data {
		res.Pairs = append(res.Pairs, &pb2.Pair{Key: k, Value: v})
	}

	log.Printf("Dumped: %s", ident)
	return res, nil
}

func (s *kvServer) Get(ctx context.Context, req *pb2.GetRequest) (*pb2.GetResponse, error) {
	k := req.Key
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
	return &pb2.GetResponse{
		Value: v,
	}, nil
}

func (s *kvServer) Put(ctx context.Context, req *pb2.PutRequest) (*pb2.PutResponse, error) {
	k := req.Key
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
	return &pb2.PutResponse{}, nil
}

func init() {
	// Ensure that nodeServer implements the NodeServer interface
	var ns *nodeServer = nil
	var _ pb.NodeServer = ns

	// Ensure that kvServer implements the KVServer interface
	var kvs *kvServer = nil
	var _ pb2.KVServer = kvs

}

func main() {
	n := Node{
		data:   make(map[rangeIdent]*RangeData),
		ranges: NewRanges(),
	}

	addr := flag.String("addr", ":9000", "address to listen on")
	flag.Parse()

	ns := nodeServer{node: &n}
	kv := kvServer{node: &n}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("control plane listening on %q", *addr)

	var opts []grpc.ServerOption
	s := grpc.NewServer(opts...)
	pb.RegisterNodeServer(s, &ns)
	pb2.RegisterKVServer(s, &kv)

	// Register reflection service, so client can introspect (for debugging).
	reflection.Register(s)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// block forever.
	select {}
}
