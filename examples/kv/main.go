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
	scope := string(bytes.TrimRight(i[:32], "\x00"))
	key := binary.LittleEndian.Uint64(i[32:])

	if scope == "" {
		return fmt.Sprintf("%d", key)
	}

	return fmt.Sprintf("%s/%d", scope, key)
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

func (rs *Ranges) Find(k key) (rangeIdent, bool) {
	for _, r := range rs.ranges {
		if bytes.Compare(k, r.start) >= 0 && bytes.Compare(k, r.end) < 0 {
			return r.ident, true
		}
	}

	return rangeIdent{}, false
}

type RangeState uint8

const (
	rsUnknown RangeState = iota
	rsFetching
	rsReady
	rsTaken
)

// This is all specific to the kv example. Nothing generic in here.
type RangeData struct {
	data  map[string][]byte
	state RangeState
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
	_, ok := n.node.data[rm.ident]
	if ok {
		return nil, fmt.Errorf("already have ident: %s", rm.ident)
	}

	rd := &RangeData{
		data:  make(map[string][]byte),
		state: rsUnknown,
	}

	// if this range has a host, it is currently assigned to some other node.
	// since we have been given the range, it has (presumably) already been set
	// as read-only on the current host.
	if req.Host != nil {
		panic("not implemented")

	} else if req.Parents != nil && len(req.Parents) > 0 {
		panic("not implemented")

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

func (s *nodeServer) Take(ctx context.Context, req *pb.TakeRequest) (*pb.TakeResponse, error) {
	pbr := req.Range
	if pbr == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	ident, err := parseIdent(req.Range)
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

	if rd.state != rsReady {
		return nil, status.Error(codes.FailedPrecondition, "can only take ranges in the READY state")
	}

	rd.state = rsTaken

	log.Printf("Taken: %s", ident)
	return &pb.TakeResponse{}, nil
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

	if rd.state != rsReady && rd.state != rsTaken {
		return nil, status.Error(codes.FailedPrecondition, "can only GET from ranges in the READY or TAKEN states")
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
