package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"

	pb "github.com/adammck/ranger/pkg/proto/gen"
)

type RangerNode interface {
	Give(r pb.GiveRequest) pb.GiveResponse
}

type Node struct {
	data map[string][]byte
	mu   sync.Mutex // guards data
}

// ---- grpc control plane

type nodeServer struct {
	pb.UnimplementedNodeServer
	node *Node
}

func (n *nodeServer) Give(ctx context.Context, req *pb.GiveRequest) (*pb.GiveResponse, error) {
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
	v, ok := h.node.data[k]
	h.node.mu.Unlock()

	if !ok {
		http.Error(w, "404: Not found", http.StatusNotFound)
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
	}

	h.node.mu.Lock()
	h.node.data[k] = body
	h.node.mu.Unlock()

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
		data: make(map[string][]byte),
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
