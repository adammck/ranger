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

// Doesn't have a mutex, since that probably happens outside, to synchronize with other structures.
type Ranges struct {
	ranges []pb.Range
}

func NewRanges() Ranges {
	return Ranges{ranges: make([]pb.Range, 0)}
}

func (rs *Ranges) Add(r pb.Range) error {
	rs.ranges = append(rs.ranges, r)
	return nil
}

func (rs *Ranges) Find(k key) (rangeIdent, bool) {
	for _, v := range rs.ranges {
		if bytes.Compare(k, v.Start) >= 0 && bytes.Compare(k, v.End) < 0 {

			// TODO: move this somewhere else, can't be invalid idents in here.
			ident, err := parseIdent(v.Ident)
			if err != nil {
				panic("invalid ident!")
			}

			return ident, true
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

type nodeServer struct {
	pb.UnimplementedNodeServer
	node *Node
}

// TODO: most of this can be moved into the lib?
func (n *nodeServer) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
	r := req.GetRange()
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing: range")
	}

	ident, err := parseIdent(req.Range.GetIdent())
	if err != nil {
		return nil, err
	}

	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	_, ok := n.node.data[ident]
	if ok {
		// todo: format ident in error message
		return nil, fmt.Errorf("already have range: %q", ident)
	}

	n.node.ranges.Add(*r)
	n.node.data[ident] = make(map[string][]byte)
	log.Printf("given range %q", ident)

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
	var rs *nodeServer = nil
	var _ pb.NodeServer = rs
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

		lis, err := net.Listen("tcp", *cp)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Printf("grpc control plane listening on %q", *cp)

		var opts []grpc.ServerOption
		s := grpc.NewServer(opts...)
		pb.RegisterNodeServer(s, &ns)

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
