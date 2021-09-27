package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb2 "github.com/adammck/ranger/examples/kv/proto/gen"
	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type RangerNode interface {
	Give(r pb.GiveRequest) pb.GiveResponse
}

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

// See also pb.Range.
type Range struct {

	// Always need this. Move to the lib?
	ident rangeIdent
	start []byte
	end   []byte

	// Just for kv example; other systems may handle transfer differently.
	writeable bool
	readable  bool
}

func parseRange(pbr *pb.Range) (Range, error) {
	ident, err := parseIdent(pbr.Ident)
	if err != nil {
		return Range{}, err
	}

	return Range{
		ident: ident,
		start: pbr.Start,
		end:   pbr.End,
	}, nil
}

// Doesn't have a mutex, since that probably happens outside, to synchronize with other structures.
type Ranges struct {
	ranges []Range
}

func NewRanges() Ranges {
	return Ranges{ranges: make([]Range, 0)}
}

func (rs *Ranges) Add(r Range) error {
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

type Node struct {
	data   map[rangeIdent]map[string][]byte
	ranges Ranges
	mu     sync.Mutex // guards data and ranges, todo: split into one for ranges, and one for each range in data
}

// ---- grpc control plane

type kvServer struct {
	pb2.UnimplementedKVServer
	node *Node
}

func (s *kvServer) Dump(ctx context.Context, req *pb2.DumpRequest) (*pb2.DumpResponse, error) {

	// lol
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	ident := [40]byte{}
	copy(ident[:], req.Range)

	_, ok := s.node.data[ident]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "range not found")
	}

	res := &pb2.DumpResponse{}
	for k, v := range s.node.data[ident] {
		res.Pairs = append(res.Pairs, &pb2.Pair{Key: k, Value: v})
	}

	return res, fmt.Errorf("not implemented")
}

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

	r, err := parseRange(pbr)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	// TODO: Look in Ranges instead here?
	_, ok := n.node.data[r.ident]
	if ok {
		return nil, fmt.Errorf("already have ident: %s", r.ident)
	}

	n.node.ranges.Add(r)
	n.node.data[r.ident] = make(map[string][]byte)
	log.Printf("given range %s", r.ident)

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
		r.readable = true
		r.writeable = true
	}

	return &pb.GiveResponse{}, nil
}

// ---- http data plane

type getHandler struct {
	node *Node
}

func (h *getHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k, ok := vars["key"]
	if !ok {
		http.Error(w, "Missing param: key", http.StatusNotFound)
		return
	}

	h.node.mu.Lock()
	defer h.node.mu.Unlock()

	ident, ok := h.node.ranges.Find(key(k))
	if !ok {
		http.Error(w, "404: No such range", http.StatusNotFound)
		return
	}

	v, ok := h.node.data[ident][k]
	if !ok {
		http.Error(w, "404: No such key", http.StatusNotFound)
		return
	}

	log.Printf("get %q", k)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(v)
}

type putHandler struct {
	node *Node
}

func (h *putHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k, ok := vars["key"]
	if !ok {
		http.Error(w, "Missing param: key", http.StatusNotFound)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %s", err), http.StatusBadRequest)
		return
	}

	h.node.mu.Lock()
	defer h.node.mu.Unlock()

	ident, ok := h.node.ranges.Find(key(k))
	if !ok {
		http.Error(w, "404: No such range", http.StatusNotFound)
		return
	}

	h.node.data[ident][k] = body

	log.Printf("put %q", k)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "200: OK")
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
		data:   make(map[rangeIdent]map[string][]byte),
		ranges: NewRanges(),
	}

	cp := flag.String("cp", ":9000", "address to listen on (grpc control plane)")
	dp := flag.String("dp", ":8000", "address to listen on (http data plane)")
	flag.Parse()

	// init grpc control plane
	go func() {
		ns := nodeServer{node: &n}
		kv := kvServer{node: &n}

		lis, err := net.Listen("tcp", *cp)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Printf("grpc control plane listening on %q", *cp)

		var opts []grpc.ServerOption
		s := grpc.NewServer(opts...)
		pb.RegisterNodeServer(s, &ns)
		pb2.RegisterKVServer(s, &kv)

		// Register reflection service, so client can introspect (for debugging).
		reflection.Register(s)

		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
	}()

	// init http data plane
	go func() {
		gh := getHandler{node: &n}
		ph := putHandler{node: &n}
		r := mux.NewRouter()
		r.Handle("/{key}", &gh).Methods("GET")
		r.Handle("/{key}", &ph).Methods("PUT")
		log.Printf("http data plane listening on %q", *dp)
		http.ListenAndServe(*dp, r)
	}()

	// block forever.
	select {}
}
